const { MongoMemoryServer } = require('mongodb-memory-server');
const mongoose = require('mongoose');

let mongod;

module.exports = {
    connect: async () => {
        mongod = await MongoMemoryServer.create();
        const uri = mongod.getUri();
        await mongoose.connect(uri);  // Remove deprecated options
    },
    closeDatabase: async () => {
        await mongoose.connection.dropDatabase();
        await mongoose.connection.close();
        await mongod.stop();
    },
    clearDatabase: async () => {
        const collections = mongoose.connection.collections;
        for (const key in collections) {
            const collection = collections[key];
            await collection.deleteMany({});
        }
    }
};
