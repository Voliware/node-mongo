const EventEmitter = require('events');
const MongoClient = require('mongodb').MongoClient;
const Logger = require('@voliware/logger');
const Collection = require('./collection');

/**
 * Controls a mongo client, db, and collections.
 * @extends {EventEmitter}
 */
class Database extends EventEmitter {

    /**
     * Constructor
     * @param {String|Object} options
     * @param {String} [options.name="admin"]
     * @param {String} [options.host="localhost"]
     * @param {String} [options.port=27017]
     * @param {String} [options.username=""]
     * @param {String} [options.password=""]
     * @param {String} [options.url=null] - Fully formed URL, other params ignored
     * @param {String[]} [collection_names=[]]
     */
    constructor({
        name = "admin",
        host = "localhost",
        port = 27017,
        username = "",
        password = "",
        url = null,
        collection_names = []
    })
    {
        super();

        /**
         * Name of the database
         * @type {String}
         */
        this.name = name;

        /**
         * Address of the database server
         * @type {String}
         */
        this.host = host;

        /**
         * Port of the database server
         * @type {Number}
         */
        this.port = port;

        /**
         * Username for the database server
         * @type {String}
         */
        this.username = username;

        /**
         * Password for the database server
         * @type {String}
         */
        this.password = password;

        /**
         * List of collections in the database.
         * Collections will either be retrieved if they exist already
         * or they will be created if they do not.
         * @type {String[]}
         */
        this.collection_names = collection_names;

        /**
         * Logging object
         * @type {Logger}
         */
        this.logger = new Logger(this.constructor.name, {level: "verbose"});

        /**
         * Fully written URL to connect to the server
         * @type {String}
         */
        this.url = typeof url === "string" 
            ? url 
            : this.createUrl({
                username: this.username,
                password: this.password,
                host: this.host,
                port: this.port,
                name: this.name
            });      

        /**
         * MongoDB client
         * @type {MongoClient}
         */
        this.client = new MongoClient(this.url, {
            useNewUrlParser: true,
            useUnifiedTopology: true
        });

        /**
         * Database connection
         * @type {Db}
         */
        this.db = null;

        /**
         * Map of Collection objects
         * @type {Map<Collection>}
         */
        this.collections = new Map();
    }

    /**
     * Create a mongodb url string with the following pattern:
     * mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
     * @param {Object} params
     * @param {String} params.username 
     * @param {String} params.password
     * @param {String} params.host
     * @param {String} params.port
     * @param {String} params.name
     * @returns {String}
     */
    createUrl({username, password, host, port, name}){
        let url = "mongodb://";

        // optional username/password, both must be set
        if(typeof username === "string" && username.length){
            if(typeof password === "string" && password.length) {
                url += `${username}:${password}@`;
            }
        }
       
        url += `${host}:${port}`;

        if(typeof name === "string" && name.length){
            url += `/${name}`;
        }

        this.logger.verbose("Generated url " + url);

        return url;
    }

    /**
     * Connect to mongo.
     * Throws an error if it fails.
     * @async
     * @returns {Promise<Boolean>} True if it connects
     */
    async connect(){
        this.logger.debug(`Connecting to ${this.url}`)
        await this.client.connect();            
        if(this.client.isConnected()){
            this.logger.info(`Connected to ${this.url}`);
            return true;
        }
        else {
            throw new Error(`Failed to connect to ${this.url}`);
        }
    }

    /**
     * Get a database
     * @param {String} name 
     * @returns {Db}
     */
    getDatabase(name){
        return this.client.db(name);
    }

    /**
     * Create all collections that do not exist.
     * Should only be called after getCollections().
     * @param {String[]} collections - Array of collection names
     * @returns {Promise}
     */
    async createCollections(collections){
        let missing = [];
        let found = false;
        for(let i = 0; i < collections.length; i++){
            let name = collections[i];
            for (const [key, value] of this.collections) {
                if(name === key){
                    found = true;
                    break;
                }
            }
            if(!found){
                missing.push(name);
            }
            found = false;
        }
        for(let i = 0; i < missing.length; i++){
            await this.createCollection(missing[i]);
        }
    }

    /**
     * Get a collection stored in the collections map.
     * @param {String} name 
     * @returns {Collection|Null}
     */
    getCollection(name){
        return this.collections.get(name);
    }

    /**
     * Get all collections and add them to the collections map.
     * @async
     * @returns {Promise}
     */
    async getCollections(){
        this.logger.debug('Getting database collections');
        return new Promise((resolve, reject) => {
            this.db.listCollections().toArray((err, items) => {
                if(err){
                    this.logger.error('Failed to get database collections');
                    reject(err);
                    return;
                }
    
                if(!items.length){
                    this.logger.info('No collections found');
                    resolve()
                    return;
                }
    
                for(let k in items){
                    let name = items[k].name;
                    let collection = new Collection(this.db.collection(name));
                    this.collections.set(name, collection);
                    this.logger.debug(`Found collection ${name}`)
                }
                resolve()
                return;
            });
        });
    }

    /**
     * Create a collection
     * @returns {Promise}
     */
    createCollection(name){
        this.logger.debug(`Creating ${name} collection`);
        return this.db.createCollection(name)
            .then(() => {
                this.logger.info(`Created ${name} collection`);
                let collection = new Collection(this.db.collection(name));
                this.collections.set(name, collection);
            })
            .catch((err) => {
                this.logger.error(`Failed to create ${name} collection`);
                this.logger.error(err);
            });
    }

    /**
     * Connect to mongo using the client.
     * Get the database.
     * Get all collections.
     * @async
     * @returns {Promise<Boolean>} True if it connects
     */
    async initialize(){
        await this.connect();
        this.db = this.getDatabase(this.name);       
        await this.getCollections(); 
        await this.createCollections(this.collection_names);
        return true;
    }
}

module.exports = Database;