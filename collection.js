const EventEmitter = require('events');
const Mongo = require('mongodb');
const Logger = require('@voliware/logger');
const sanitize = require('mongo-sanitize');

/**
 * MongoDB collection wrapper
 * @extends {EventEmitter}
 */
class Collection extends EventEmitter {

    /**
     * Constructor
     * @param {Collection} collection - Native MongoDB Collection object
     */
    constructor(collection){
        super();

        /**
         * Native MongoDB Collection object
         * @type {Collection}
         */
        this.collection = collection;

        /**
         * Logging object
         * @type {Logger}
         */
        this.logger = new Logger(this.collection.collectionName, {level: "info"});
    } 

    /**
     * Process a filter object used for queries.
     * If the filter has an _id property, replace it or all nested ids
     * with a new Mongo.ObjectID.
     * @param {Object} [filter={}] 
     * @returns {Object}
     */
    processFilter(filter) {
        for (let k1 in filter) {
            if (k1 === '_id') {
                switch (typeof filter[k1]) {
                    // Basic string such as {_id: 1}
                    case 'string':
                        filter[k1] = new Mongo.ObjectID(filter[k1]);
                    break;
                    case 'object':
                        // Nested strings such as {_id: {$in: [0,1,2]}} 
                        for(let k2 in filter[k1]){
                            if(Array.isArray(filter[k1][k2])){
                                for(let i = 0; i < filter[k1][k2].length; i++){
                                    filter[k1][k2][i] = new Mongo.ObjectID(filter[k1][k2][i]);
                                }
                            }
                        }
                    break;
                }
            }
        }
        return filter;
    }
    
    /**
     * Sanitize a filter against NoSQL injection.
     * This should only be used for external filters.
     * @param {Object} [filter={}] 
     * @returns {Object}
     */
    sanitizeFilter(filter={}){
        if(Array.isArray(filter)){
            filter.forEach((elm) => {
                this.sanitizeFilter(elm)
            });
        }
        if(typeof(filter) === 'object' && filter !== null){
            Object.values(filter).forEach((elm) => {
                this.sanitizeFilter(elm)
            });
        }
        return sanitize(filter);
    }

    /**
     * Delete a document
     * @param {Object} filter - Query filter
     * @param {Object} [options={}] - Query options
     * @async
     * @returns {Promise<Boolean>} True if deleted
     */
    async deleteDocument(filter, options={}){
        this.logger.debug('Deleting document');
        this.logger.verbose(filter);
        this.logger.verbose(options);
        filter = this.processFilter(filter);
        let result = await this.collection.deleteOne(filter);
        if(result.deletedCount){
            this.logger.info('Deleted document');
            return true;
        }
        else {
            this.logger.error('Failed to delete document');
            return false;
        }
    }

    /**
     * Find a document
     * @param {Object} filter - Query filter
     * @param {Object} [options={}] - Query options
     * @async
     * @returns {Promise<Object>} Document
     */
    async getDocument(filter, options={}){
        this.logger.debug('Getting document');
        this.logger.verbose(filter);
        this.logger.verbose(options);
        filter = this.processFilter(filter);
        let item = await this.collection.findOne(filter, options)
        if(item){
            this.logger.debug('Got document');
        }
        else {
            this.logger.debug('Failed to find document');
        }
        return item;
    }

    /**
     * Get documents
     * @param {Object} [filter] 
     * @param {Object} [options={}] 
     * @param {Number} [options.limit=50] 
     * @async
     * @returns {Promise<Object[]}
     */
    async getDocuments(filter={}, options={limit:10}){
        this.logger.debug('Getting documents');
        this.logger.verbose(filter);
        this.logger.verbose(options);
        filter = this.processFilter(filter);
        let cursor = await this.collection.find(filter, options);
        return new Promise((resolve, reject) => {
            cursor.toArray((error, result) => {
                if(error){
                    reject(error)
                }
                else {
                    resolve(result);
                }
            });
        });
    }

    /**
     * Get document count
     * @param {Object} [filter] 
     * @param {Object} [options={}] 
     * @returns {Promise<Number}
     */
    async getCount(filter={}, options={}){
        this.logger.debug('Getting document count');
        this.logger.verbose(options);
        return this.collection.countDocuments(filter, options);
    }

    /**
     * Insert a document
     * @param {Object} document
     * @param {Object} [options={}] 
     * @async
     * @returns {Promise<Object>}
     */
    async insertDocument(document, options={}){
        this.logger.debug('Inserting document');
        this.logger.verbose(document);
        this.logger.verbose(options);
        let result = await this.collection.insertOne(document, options)
        if(result.insertedCount){
            this.logger.info(`Inserted document ${result.insertedId}`);
        }
        else {
            this.logger.error('Failed to insert document');
        }
        return result;
    }

    /**
     * Replace a document
     * @param {Object} filter
     * @param {Object} document
     * @param {Object} [options={}] 
     * @async
     * @returns {Promise<Boolean>}
     */
    async replaceDocument(filter, document, options={}){
        delete document._id;
        this.logger.debug('Replacing document');
        this.logger.verbose(filter);
        this.logger.verbose(document);
        this.logger.verbose(options);
        filter = this.processFilter(filter);
        let result = await this.collection.replaceOne(filter, document, options)
        if(result.modifiedCount){
            this.logger.info('Replaced document');
            return true;
        }
        else {
            this.logger.error('Failed to replace document');
            return false;
        }
    }

    /**
     * Update a document
     * @param {Object} filter
     * @param {Object} update
     * @param {Object} [options={}]
     * @async
     * @returns {Promise<boolean>}
     */
    async updateDocument(filter, update, options={}){
        delete update._id;
        this.logger.debug('Updating document');
        this.logger.verbose(filter);
        this.logger.verbose(update);
        this.logger.verbose(options);
        filter = this.processFilter(filter);
        let result = await this.collection.updateOne(filter, update, options)
        if(result.modifiedCount){
            this.logger.info('Updated document');
            return true;
        }
        else {
            this.logger.warning('Failed to update document');
            return false;
        }
    }

    /**
     * Drop the collection
     * @returns {Promise}
     */
    drop(){
        this.logger.debug('Dropping collection');
        return this.collection.drop()
            .then(() => {
                this.logger.info('Dropped collection');
            })
            .catch((err) => {
                this.logger.error('Failed to drop collection');
                this.logger.error(err);
            });
    }

    /**
     * Wipe the collection
     * @returns {Promise}
     */
    wipe(){
        this.logger.debug('Wiping collection');
        return this.collection.deleteMany({})
            .then(() => {
                this.logger.info('Wiped collection');
            })
            .catch((err) => {
                this.logger.error('Failed to wipe collection');
                this.logger.error(err);
            });
    }
}

module.exports = Collection;