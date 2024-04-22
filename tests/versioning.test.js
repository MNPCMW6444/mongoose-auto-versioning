const mongoose = require('mongoose');
const { connect, closeDatabase, clearDatabase } = require('./setup');
const {
    cloneSchema,
    isWritable,
    isValidVersion,
    versioning,
    constants
} = require('../src/index');
beforeAll(async () => await connect(), 20000); // Increased timeout
afterEach(async () => await clearDatabase());
afterAll(async () => await closeDatabase(), 20000); // Increased timeout


describe('Schema Manipulation and Document Versioning', () => {
    // Define a simple schema
    const TestSchema = new mongoose.Schema({ name: String, age: Number, deleted: Boolean });
    const TestModel = mongoose.model('Test', TestSchema);

    describe('cloneSchema', () => {
        it('should clone a schema without the _id field', async () => {
            const clonedSchema = cloneSchema(TestSchema, mongoose);
            expect(clonedSchema.paths._id).toBeUndefined();
        });

        it('should set unique to false and required to false for all fields except VERSION', async () => {
            const clonedSchema = cloneSchema(TestSchema, mongoose);
            Object.keys(clonedSchema.paths).forEach(path => {
                if (path !== '_version') {
                    expect(clonedSchema.paths[path].options.unique).toBe(false);
                    expect(clonedSchema.paths[path].options.required).toBe(false);
                }
            });
        });
    });

    describe('isWritable', () => {
        it('should return false for reserved fields', () => {
            expect(isWritable('_editor')).toBe(false);
        });

        it('should return true for non-reserved fields', () => {
            expect(isWritable('name')).toBe(true);
        });
    });

    describe('isValidVersion', () => {
        it('should reject non-string inputs', () => {
            expect(isValidVersion(123)).toBe(false);
        });

        it('should reject non-numeric strings', () => {
            expect(isValidVersion('abc')).toBe(false);
        });

        it('should accept valid version numbers', () => {
            expect(isValidVersion('1')).toBe(true);
        });
    });

    describe('Versioning Operations', () => {
        it('should create a versioned document on update', async () => {
            await TestModel.create({ name: 'Test User', age: 30 });
            await TestModel.findOneAndUpdate({ name: 'Test User' }, { age: 31 }, { new: true });

            const VersionedModel = mongoose.models['versions'];
            const history = await VersionedModel.find({});
            expect(history.length).toBe(1);
            expect(history[0].age).toBe(30);
        });

        it('should mark a document as deleted', async () => {
            const doc = await TestModel.create({ name: 'Test User', age: 30 });
            await doc.deleteOne();

            expect(doc.deleted).toBe(true);
            const found = await TestModel.findById(doc._id);
            expect(found).toBeNull();
        });
    });
});
