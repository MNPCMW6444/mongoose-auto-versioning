// jest.config.js or jest.config.ts
module.exports = {
    transform: {
        '^.+\\.(ts|tsx)$': ['ts-jest', {
            // Your ts-jest specific configuration
            tsconfig: './tsconfig.json'
        }],
    },
    // other jest configurations like test environment
    testEnvironment: 'node',
};
