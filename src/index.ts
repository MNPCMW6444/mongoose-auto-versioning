import {ObjectId} from "mongodb"
import {fromJS} from "immutable";
import {Document} from "mongoose";

const VERSION = "_version"
const ID = "_id"
const VALIDITY = "_validity"
const EDITOR = "_editor"
const DELETER = "_deleter"
const DEFAULT_EDITOR = "default"
const DEFAULT_DELETER = "default"
const SESSION = "_session"

const RESERVED_FIELDS = [
    VERSION,
    VALIDITY,
    EDITOR,
    DELETER,
    SESSION]

const constants = {
    VERSION,
    ID,
    VALIDITY,
    EDITOR,
    DELETER,
    DEFAULT_EDITOR,
    DEFAULT_DELETER,
    SESSION,
    RESERVED_FIELDS
}

const cloneSchema = (schema: any, mongoose: any) => {
    let clonedSchema = new mongoose.Schema({autoIndex: false})
    schema.eachPath(function (path: any, type: any) {
        if (path === constants.ID) {
            return
        }
        // clone schema
        let clonedPath: any = {}
        clonedPath[path] = type.options

        // shadowed props are not unique
        clonedPath[path].unique = false

        // shadowed props are not all required
        if (path !== constants.VERSION) {
            clonedPath[path].required = false
        }

        clonedSchema.add(clonedPath)
    })
    return clonedSchema
}

const isWritable = (field: any) => {
    return !constants.RESERVED_FIELDS.find(
        key => key === field
    )
}

const isValidVersion = (v: any) => {
    if (typeof v != "string") return false // we only process strings!
    if (isNaN(v as any)) return false // use type coercion to parse the _entirety_ of the string (`parseFloat` alone does not do this)...
    if (isNaN(parseInt(v))) return false// ...and ensure strings of whitespace fail
    if (parseInt(v) < 1) return false
    return true
}


const filterAndModifyOne = async (query: any, next: any) => {

    // load the base version
    let base = await queryOne(query, next)
    if (base === null) next()
    else {
        // get the transaction session
        const session = query.options.session

        // store the session for the save method
        base[constants.SESSION] = session

        if (!query._update) {
            // special case for delete operations
            base[constants.DELETER] = query.options[constants.DELETER] || constants.DEFAULT_DELETER
        } else {
            // special case for update operations
            base[constants.EDITOR] = query.options[constants.EDITOR] || constants.DEFAULT_EDITOR
        }

        await base.save({session})

        // special case for the replace document, avoid the version to get reseted to zero
        if ((query._update) && (!query._update["$set"])) {
            query._update[constants.VERSION] = base[constants.VERSION]
            query._update[constants.VALIDITY] = base[constants.VALIDITY]
        }

    }
    next()
}

const filterAndModifyMany = async (query: any, next: any) => {

    // load the base version
    let bases = await query.model
        .find(query._conditions)

    // get the transaction session
    const session = query.options.session

    for (const base of bases) {

        // store the session for the save method
        base[constants.SESSION] = session

        if (!query._update) {
            // special case for delete operations
            base[constants.DELETER] = query.options[constants.DELETER] || constants.DEFAULT_DELETER
        } else {
            // special case for update operations
            base[constants.EDITOR] = query.options[constants.EDITOR] || constants.DEFAULT_EDITOR
        }

        await base.save({session})
    }
    next()
}

const getQueryOptions = (query: any) => {
    // only for findOneAndUpdate
    let sort = {}
    let skip = 0

    if (query.op.startsWith("find")) {
        sort = query.options.sort || {}
    }

    return {sort, skip}
}

const queryOne = async (query: any, next: any) => {
    // load the base version
    let base = await query.model.findOne(query._conditions, null, getQueryOptions(query))
    return base
}


module.exports = function (schema: any, options: any) {

    //Handling of the options (inherited from vermongo)
    if (typeof (options) == 'string') {
        options = {
            collection: options
        }
    }

    options = options || {}
    options.collection = options.collection || 'versions'
    options.logError = options.logError || false
    options.ensureIndex = options.ensureIndex ?? true
    options.mongoose = options.mongoose || require('mongoose')
    const mongoose = options.mongoose
    const connection = options.connection || mongoose

    // Make sure there's no reserved paths
    constants.RESERVED_FIELDS.map(
        key => {
            if (schema.path(key)) throw Error(`Schema can't have a path called "${key}"`)
        }
    )

    // create the versioned schema
    let versionedSchema = cloneSchema(schema, mongoose)

    // Copy schema options in the versioned schema
    Object.keys(options).forEach(key => versionedSchema.set(key, options[key]));

    // Define Custom fields
    let validityField: any = {}
    validityField[constants.VALIDITY] = {
        start: {type: Date, required: true, default: Date.now},
        end: {type: Date, required: false}
    }

    let versionedValidityField: any = {}
    versionedValidityField[constants.VALIDITY] = {
        start: {type: Date, required: true},
        end: {type: Date, required: true}
    }

    let versionField: any = {}
    versionField[constants.VERSION] = {type: Number, required: true, default: 0, select: true}

    let versionedIdField: any = {}
    versionedIdField[constants.ID] = mongoose.Schema.Types.Mixed
    versionedIdField[constants.VERSION] = versionField[constants.VERSION]

    let editorField: any = {}
    editorField[constants.EDITOR] = {type: String, required: false}

    let deleterField: any = {}
    deleterField[constants.DELETER] = {type: String, required: false}

    // Add Custom fields
    schema.add(validityField)
    schema.add(versionField)
    schema.add(editorField)
    schema.add(deleterField)

    versionedSchema.add(versionField)
    versionedSchema.add(versionedIdField)
    versionedSchema.add(versionedValidityField)
    versionedSchema.add(editorField)
    versionedSchema.add(deleterField)

    // add index to versioning (id, validity),
    const validity_end = constants.VALIDITY + ".end"
    const validity_start = constants.VALIDITY + ".start"

    let versionedValidityIndex: any = {}
    versionedValidityIndex[constants.ID + '.' + constants.ID] = 1
    versionedValidityIndex[validity_start] = 1
    versionedValidityIndex[validity_end] = 1
    const indexName = {name: "_id_validity_start_validity_end"};
    versionedSchema.index(versionedValidityIndex, indexName)

    // Turn off internal versioning, we don't need this since we version on everything
    schema.set("versionKey", false)
    versionedSchema.set("versionKey", false)

    // Add reference to model to original schema
    schema.statics.VersionedModel = connection.model(options.collection, versionedSchema)

    // calling create index from MongoDB to be sure index is created
    if (options.ensureIndex)
        schema.statics.VersionedModel.collection.createIndex(versionedValidityIndex, indexName)

    // Add special find by id and validity date that includes versioning
    schema.statics.findValidVersion = async (id: any, date: any, model: any) => {

        // 1. check if in current collection is valid
        const validity_end = constants.VALIDITY + ".end"
        const validity_start = constants.VALIDITY + ".start"

        let query: any = {"_id": new ObjectId(id)}
        query[validity_start] = {$lte: date}

        let current = await model.findOne(query)
        if (current)
            return current

        // 2. if not, check versioned collection
        let versionedModel = schema.statics.VersionedModel
        query = {}
        query[constants.ID + "." + constants.ID] = new ObjectId(id)
        query[validity_start] = {$lte: date}
        query[validity_end] = {$gt: date}

        let version = await versionedModel.findOne(query)
        return version
    }

    // Add special find by id and version number that includes versioning
    schema.statics.findVersion = async (id: any, version: any, model: any) => {

        // 1. check if version is the main collection
        let query: any = {}
        query[constants.ID] = new ObjectId(id)
        query[constants.VERSION] = version

        let current = await model.findOne(query)
        if (current) {
            {
                return current
            }
        }

        // 2. if not, check versioned collection
        let versionedModel = schema.statics.VersionedModel
        query = {}
        let versionedId: any = {}
        versionedId[constants.ID] = new ObjectId(id)
        versionedId[constants.VERSION] = version
        query[constants.ID] = versionedId

        let document = await versionedModel.findOne(query)
        return document
    }

    // Add special bulk save that supports versioning, note that the
    // documents are the updated documents and the originals a clone (simple JS object) of what is
    // already in the DB
    schema.statics.bulkSaveVersioned = async (documents: any, originals: any, model: any, options = {}) => {

        // check inputs have the proper length
        if (documents.length != originals.length && originals.length > 0) {
            let err = new Error('documents and originals lengths do not match')
            throw (err)
        }

        const now = new Date()
        // loop over the inputs to create a bulk write set
        for (let i = 0; i < documents.length; i += 1) {

            // Set fields for update
            documents[i][constants.VALIDITY] = {"start": now}

            if (originals.length > 0) {
                // create the versioned
                originals[i] = new schema.statics.VersionedModel(originals[i])

                // remove editor info
                originals[i][constants.EDITOR] = documents[i][constants.EDITOR] || constants.DEFAULT_EDITOR
                delete documents[i][constants.EDITOR]

                // set fields for original
                originals[i][constants.VALIDITY]["end"] = now

                let versionedId: any = {}
                versionedId[constants.ID] = originals[i][constants.ID]
                versionedId[constants.VERSION] = originals[i][constants.VERSION]
                originals[i][constants.ID] = versionedId

                // check and increase version number
                if (documents[i][constants.VERSION] == originals[i][constants.VERSION]) {
                    documents[i][constants.VERSION] = documents[i][constants.VERSION] + 1
                } else {
                    let err = new Error('document and original versions do not match for _id: ' + documents[i]._id)
                    throw (err)
                }
            } else {
                documents[i][constants.VERSION] = 1
            }
        }

        let resUpdated = undefined
        let resVersioned = undefined

        if (originals.length > 0) {
            //call buildBulkWriteOperations for the modified documents to avoid middleware hooks
            let ops = model.buildBulkWriteOperations(documents, {skipValidation: true});
            resUpdated = await model.bulkWrite(ops, options)

            // call mongoos bulkSave since the versioned collection has no middleware hooks
            let versionedModel = schema.statics.VersionedModel
            resVersioned = await versionedModel.bulkSave(originals, options)

            // raise an error if not all the documents were modified
            if (resUpdated.nModified < documents.length) {
                let err = new Error('bulk update failed, only ' + resUpdated.nModified + ' out of ' + documents.length + ' were updated')
                throw (err)
            }

        } else {
            resUpdated = await model.bulkSave(documents, options)
        }

        return resUpdated
    }

    // Add special find by id and version number that includes versioning
    schema.statics.bulkDeleteVersioned = async (documents: any, model: any, options = {}) => {

        const now = new Date()
        let versionedModel = schema.statics.VersionedModel

        // loop over the inputs to create a bulk deletr set
        let ops = []
        for (let i = 0; i < documents.length; i += 1) {
            documents[i] = new versionedModel(fromJS(documents[i].toObject()).toJS())

            // Set fields for versioned
            documents[i][constants.VALIDITY]["end"] = now
            documents[i][constants.DELETER] = documents[i][constants.DELETER] || constants.DEFAULT_DELETER

            let versionedId: any = {}
            versionedId[constants.ID] = documents[i][constants.ID]
            versionedId[constants.VERSION] = documents[i][constants.VERSION]
            documents[i][constants.ID] = versionedId

            let op = {
                "deleteOne": {
                    "filter": {"_id": documents[i]._id}
                }
            }

            ops.push(op)
        }

        let resDeleted = undefined
        let resVersioned = undefined

        // delete on the main collection
        resDeleted = await model.bulkWrite(ops, options)

        // raise an error if not all the documents were modified
        if (resDeleted.nRemoved < documents.length) {
            let err = new Error('bulk delete failed, only ' + resDeleted.nRemoved + ' out of ' + documents.length + ' were removed')
            throw (err)
        }

        // save latest version in the versioned collection
        resVersioned = await versionedModel.bulkSave(documents, options)

        return resDeleted
    }

    // document level middleware
    schema.pre('save', async function (this: Document, next: any) {

        if ((this).isNew) {
            (this)[constants.VERSION as keyof Document] = 1
            return next()
        }

        let baseVersion = (this as any)[constants.VERSION]
        // load the base version
        let base = await this.collection
            .findOne({[constants.ID]: this[constants.ID as keyof Document]})
            .then((foundBase: any) => {
                if (foundBase === null) {
                    let err = new Error('document to update not found in collection')
                    throw (err)
                }
                return foundBase
            })

        let bV = base[constants.VERSION]
        if (baseVersion !== bV) {
            let err = new Error('modified and base versions do not match')
            throw (err)
        }

        // get the transaction session
        const session = {session: this[("_session" as keyof Document)]}
        delete this[("_session" as keyof Document)]

        // clone base document to create an archived version
        let clone = fromJS(base).toJS()

        // Build Vermongo historical ID
        clone[constants.ID] = {
            [constants.ID]: this[constants.ID as keyof Document],
            [constants.VERSION]: this[constants.VERSION as keyof Document]
        }

        // Set validity to end now for versioned and to start now for current
        const now = new Date()
        const start = base[constants.VALIDITY]["start"]

        clone[constants.VALIDITY] = {
            "start": start,
            "end": now
        }

        this[constants.VALIDITY as keyof Document] = {"start": now}

        // Special case for the findAndDelete to include deleter information
        if (this[constants.DELETER as keyof Document]) {
            clone[constants.DELETER] = this[constants.DELETER as keyof Document]
        }
        // store edition info
        else {
            let editor_info = this[constants.EDITOR as keyof Document] || constants.DEFAULT_EDITOR
            this[constants.EDITOR as keyof Document] = undefined
            clone[constants.EDITOR] = editor_info
        }

        // Increment version number
        this[constants.VERSION as keyof Document] = this[constants.VERSION as keyof Document] + 1

        // Save versioned document
        var versionedDoc = new schema.statics.VersionedModel(clone)
        await versionedDoc.save(session)
        next()
        return null
    })

    schema.pre('remove', async function (this: Document, next: any) {

        // get the transaction session
        const session = {session: this[("_session" as keyof Document)]}
        delete this[("_session" as keyof Document)]

        // save current version clone in shadow collection
        let clone = fromJS(this.toObject()).toJS()

        // Build Vermongo historical ID
        clone[constants.ID] = {
            [constants.ID]: this[constants.ID as keyof Document],
            [constants.VERSION]: this[constants.VERSION as keyof Document]
        }

        const now = new Date()
        const start = this[constants.VALIDITY as keyof Document]["start"]
        clone[constants.VALIDITY] = {
            "start": start,
            "end": now
        }

        clone[constants.DELETER] = this[constants.DELETER as keyof Document] || constants.DEFAULT_DELETER

        await new schema.statics.VersionedModel(clone).save(session)

        next()
        return null
    })

    // model level middleware
    schema.pre('insertMany', async function (next: any, docs: any) {
        docs.forEach((d: any) => {
            d[constants.VERSION] = 1;
        })
        next()
    })

    //updateOne (includes document and model/query level)
    schema.pre('updateOne', async function (this: Document, next: any) {
        await filterAndModifyOne(this, next)
    })

    //updateMany (query level)
    schema.pre('updateMany', async function (this: Document, next: any) {
        await filterAndModifyMany(this, next)
    })

    // findOneAndUpdate (query level)
    schema.pre('findOneAndUpdate', async function (this: Document, next: any) {
        await filterAndModifyOne(this, next)
    })

    // findOneAndReplace (query level)
    schema.pre('findOneAndReplace', async function (this: Document, next: any) {
        await filterAndModifyOne(this, next)
    })

    // findOneAndReplace (query level)
    schema.pre('replaceOne', async function (this: Document, next: any) {
        await filterAndModifyOne(this, next)
    })

    //deleteOne (includes document and model/query level)
    schema.pre('deleteOne', async function (this: Document, next: any) {
        await filterAndModifyOne(this, next)
    })

    //findOneAndRemove (query level)
    schema.pre('findOneAndRemove', async function (this: Document, next: any) {
        await filterAndModifyOne(this, next)
    })

    //findOneAndRemove (query level)
    schema.pre('findOneAndDelete', async function (this: Document, next: any) {
        await filterAndModifyOne(this, next)
    })

    //deleteMany (query level)
    schema.pre('deleteMany', async function (this: Document, next: any) {
        await filterAndModifyMany(this, next)
    })
}