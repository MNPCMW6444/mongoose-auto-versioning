import { Document, Schema } from "mongoose";

export const VERSION = "_version";
export const ID = "_id";
export const VALIDITY = "_validity";
export const EDITOR = "_editor";
export const DELETER = "_deleter";
export const DEFAULT_EDITOR = "default";
export const DEFAULT_DELETER = "default";
export const SESSION = "_session";

export const RESERVED_FIELDS = [VERSION, VALIDITY, EDITOR, DELETER, SESSION];

export const constants = {
    VERSION, ID, VALIDITY, EDITOR, DELETER, DEFAULT_EDITOR, DEFAULT_DELETER, SESSION, RESERVED_FIELDS
};




export const cloneSchema = (schema: Schema<any>, mongoose: any): Schema<any> => {
    let clonedSchema = new mongoose.Schema({}, { autoIndex: false });
    schema.eachPath((path: string, type: any) => {
        if (path === constants.ID) {
            return;  // Ensure this actually prevents adding `_id`
        }
        let clonedPath: any = {};
        clonedPath[path] = { ...type.options, unique: false, required: (path === constants.VERSION) };
        clonedSchema.add({ [path]: clonedPath[path] });
    });
    return clonedSchema;
};

export const isWritable = (field: string): boolean => {
    return !constants.RESERVED_FIELDS.includes(field);
};

export const isValidVersion = (v: string): boolean => {
    if (typeof v !== "string") return false;
    const parsed = parseInt(v);
    return !isNaN(parsed) && parsed > 0;
};

export const filterAndModifyOne = async (query: any, next: any) => {
    let base = await queryOne(query, next);
    if (base === null) next();
    else {
        const session = query.options.session;
        base[constants.SESSION] = session;
        if (!query._update) {
            base[constants.DELETER] = query.options[constants.DELETER] || constants.DEFAULT_DELETER;
        } else {
            base[constants.EDITOR] = query.options[constants.EDITOR] || constants.DEFAULT_EDITOR;
        }
        await base.save({session});
        if ((query._update) && (!query._update["$set"])) {
            query._update[constants.VERSION] = base[constants.VERSION];
            query._update[constants.VALIDITY] = base[constants.VALIDITY];
        }
    }
    next();
}

export const filterAndModifyMany = async (query: any, next: any) => {
    let bases = await query.model.find(query._conditions);
    const session = query.options.session;
    for (const base of bases) {
        base[constants.SESSION] = session;
        if (!query._update) {
            base[constants.DELETER] = query.options[constants.DELETER] || constants.DEFAULT_DELETER;
        } else {
            base[constants.EDITOR] = query.options[constants.EDITOR] || constants.DEFAULT_EDITOR;
        }
        await base.save({session});
    }
    next();
}

export const getQueryOptions = (query: any) => {
    let sort = {};
    let skip = 0;
    if (query.op.startsWith("find")) {
        sort = query.options.sort || {};
    }
    return {sort, skip};
}

export const queryOne = async (query: any, next: any) => {
    let base = await query.model.findOne(query._conditions, null, getQueryOptions(query));
    return base;
}

export const versioning = (schema: any, options: any) => {
    if (typeof options == 'string') {
        options = {collection: options};
    }

    options = options || {};
    options.collection = options.collection || 'versions';
    options.mongoose = options.mongoose || require('mongoose');
    const mongoose = options.mongoose;
    const versionedModelName = options.collection;

    schema.add({deleted: {type: Boolean, default: false}});


    if (!mongoose.models[versionedModelName]) {
        let versionedSchema = cloneSchema(schema, mongoose);
        versionedSchema.add({originalId: mongoose.Schema.Types.ObjectId, version: Number});
        schema.statics.VersionedModel = mongoose.model(versionedModelName, versionedSchema);
    } else {
        schema.statics.VersionedModel = mongoose.models[versionedModelName];
    }

    schema.statics.getDocumentHistory = async function (originalId: any) {
        return await this.VersionedModel.find({originalId}).sort({version: -1});
    };


    schema.pre('save', async function (this: Document, next: any) {
        if (!this.isNew) {
            const previousVersion = this.toObject({versionKey: false});
            delete previousVersion._id;
            previousVersion.originalId = this._id;
            previousVersion.version = this.__v;
            await schema.statics.VersionedModel.create(previousVersion);
        }
        next();
    });

    schema.pre('findOneAndUpdate', async function (this: Document, next: any) {
        const originalDoc = await (this as any).model.findOne((this as any).getQuery()).exec();
        if (originalDoc) {
            const previousVersion = originalDoc.toObject({versionKey: false});
            delete previousVersion._id;
            previousVersion.originalId = originalDoc._id;
            previousVersion.version = originalDoc.__v;
            await schema.statics.VersionedModel.create(previousVersion);
        }
        next();
    });

    interface MyDocument extends Document {
        deleted?: boolean;
        // Do not redefine `save` unless necessary
    }



    schema.pre('deleteOne', { document: true, query: false }, async function(this: MyDocument, next: () => void) {
        this.deleted = true;
        await this.save();
        next();
    });



};
