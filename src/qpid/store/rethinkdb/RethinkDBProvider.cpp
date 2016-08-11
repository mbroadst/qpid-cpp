/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <stdlib.h>
#include <string>
#include <thread>

#include <boost/thread/tss.hpp>

#include <qpid/broker/RecoverableQueue.h>
#include <qpid/log/Statement.h>
#include <qpid/store/MessageStorePlugin.h>
#include <qpid/store/StorageProvider.h>

#include "rethinkdb.h"
namespace R = RethinkDB;

#include "RethinkDBException.h"

namespace qpid {
namespace store {
namespace rethinkdb {

// Table names
const std::string TblBinding("tblBinding");
const std::string TblConfig("tblConfig");
const std::string TblExchange("tblExchange");
const std::string TblMessage("tblMessage");
const std::string TblMessageMap("tblMessageMap");
const std::string TblQueue("tblQueue");
const std::string TblTpl("tblTPL");

const uint64_t MAX_SAFE_SEQUENCE_NUMBER = 9007199254740991; // 2^53 - 1
class IdSequence
{
    std::mutex lock;
    uint64_t id;
public:
    IdSequence() : id(1) {}
    uint64_t next() {
        std::unique_lock<std::mutex> guard(lock);
        id++;
        if (id >= MAX_SAFE_SEQUENCE_NUMBER) {
            id = 1;
        }

        return id++;
    }

    void reset(uint64_t value) {
        // deliberately not threadsafe, used only on recovery
        id = (value >= MAX_SAFE_SEQUENCE_NUMBER || value <= 0) ? 1 : value;
    }
};

/**
 * @class RethinkDBProvider
 *
 * Implements a qpid::store::StorageProvider that uses RethinkDB as
 * the backend data store for Qpid.
 */
class RethinkDBProvider : public qpid::store::StorageProvider
{
protected:
    void finalizeMe();

    void dump();

public:
    RethinkDBProvider();
    ~RethinkDBProvider();

    virtual qpid::Options* getOptions() { return &options; }

    virtual void earlyInitialize (Plugin::Target& target);
    virtual void initialize(Plugin::Target& target);

    /**
     * Receive notification that this provider is the one that will actively
     * handle provider storage for the target. If the provider is to be used,
     * this method will be called after earlyInitialize() and before any
     * recovery operations (recovery, in turn, precedes call to initialize()).
     */
    virtual void activate(MessageStorePlugin &store);

    /**
     * @name Methods inherited from qpid::broker::MessageStore
     */

    /**
     * Record the existence of a durable queue
     */
    virtual void create(PersistableQueue& queue,
                        const qpid::framing::FieldTable& args);
    /**
     * Destroy a durable queue
     */
    virtual void destroy(PersistableQueue& queue);

    /**
     * Record the existence of a durable exchange
     */
    virtual void create(const PersistableExchange& exchange,
                        const qpid::framing::FieldTable& args);
    /**
     * Destroy a durable exchange
     */
    virtual void destroy(const PersistableExchange& exchange);

    /**
     * Record a binding
     */
    virtual void bind(const PersistableExchange& exchange,
                      const PersistableQueue& queue,
                      const std::string& key,
                      const qpid::framing::FieldTable& args);

    /**
     * Forget a binding
     */
    virtual void unbind(const PersistableExchange& exchange,
                        const PersistableQueue& queue,
                        const std::string& key,
                        const qpid::framing::FieldTable& args);

    /**
     * Record generic durable configuration
     */
    virtual void create(const PersistableConfig& config);

    /**
     * Destroy generic durable configuration
     */
    virtual void destroy(const PersistableConfig& config);

    /**
     * Stores a messages before it has been enqueued
     * (enqueueing automatically stores the message so this is
     * only required if storage is required prior to that
     * point). If the message has not yet been stored it will
     * store the headers as well as any content passed in. A
     * persistence id will be set on the message which can be
     * used to load the content or to append to it.
     */
    virtual void stage(const boost::intrusive_ptr<PersistableMessage>& msg);

    /**
     * Destroys a previously staged message. This only needs
     * to be called if the message is never enqueued. (Once
     * enqueued, deletion will be automatic when the message
     * is dequeued from all queues it was enqueued onto).
     */
    virtual void destroy(PersistableMessage& msg);

    /**
     * Appends content to a previously staged message
     */
    virtual void appendContent(const boost::intrusive_ptr<const PersistableMessage>& msg,
                               const std::string& data);

    /**
     * Loads (a section) of content data for the specified
     * message (previously stored through a call to stage or
     * enqueue) into data. The offset refers to the content
     * only (i.e. an offset of 0 implies that the start of the
     * content should be loaded, not the headers or related
     * meta-data).
     */
    virtual void loadContent(const qpid::broker::PersistableQueue& queue,
                             const boost::intrusive_ptr<const PersistableMessage>& msg,
                             std::string& data,
                             uint64_t offset,
                             uint32_t length);

    /**
     * Enqueues a message, storing the message if it has not
     * been previously stored and recording that the given
     * message is on the given queue.
     *
     * Note: that this is async so the return of the function does
     * not mean the opperation is complete.
     *
     * @param msg the message to enqueue
     * @param queue the name of the queue onto which it is to be enqueued
     * @param xid (a pointer to) an identifier of the
     * distributed transaction in which the operation takes
     * place or null for 'local' transactions
     */
    virtual void enqueue(qpid::broker::TransactionContext* ctxt,
                         const boost::intrusive_ptr<PersistableMessage>& msg,
                         const PersistableQueue& queue);

    /**
     * Dequeues a message, recording that the given message is
     * no longer on the given queue and deleting the message
     * if it is no longer on any other queue.
     *
     * Note: that this is async so the return of the function does
     * not mean the opperation is complete.
     *
     * @param msg the message to dequeue
     * @param queue the name of the queue from which it is to be dequeued
     * @param xid (a pointer to) an identifier of the
     * distributed transaction in which the operation takes
     * place or null for 'local' transactions
     */
    virtual void dequeue(qpid::broker::TransactionContext* ctxt,
                         const boost::intrusive_ptr<PersistableMessage>& msg,
                         const PersistableQueue& queue);

    /**
     * Flushes all async messages to disk for the specified queue
     *
     * Note: this is a no-op for this provider.
     *
     * @param queue the name of the queue from which it is to be dequeued
     */
    virtual void flush(const PersistableQueue& /*queue*/) {};

    /**
     * Returns the number of outstanding AIO's for a given queue
     *
     * If 0, than all the enqueue / dequeues have been stored
     * to disk
     *
     * @param queue the name of the queue to check for outstanding AIO
     */
    virtual uint32_t outstandingQueueAIO(const PersistableQueue& /*queue*/)
        {return 0;}
    //@}

    /**
     * @name Methods inherited from qpid::broker::TransactionalStore
     */
    //@{
    virtual std::auto_ptr<qpid::broker::TransactionContext> begin();
    virtual std::auto_ptr<qpid::broker::TPCTransactionContext> begin(const std::string& xid);
    virtual void prepare(qpid::broker::TPCTransactionContext& txn);
    virtual void commit(qpid::broker::TransactionContext& txn);
    virtual void abort(qpid::broker::TransactionContext& txn);
    virtual void collectPreparedXids(std::set<std::string>& xids);
    //@}

    virtual void recoverConfigs(qpid::broker::RecoveryManager& recoverer);
    virtual void recoverExchanges(qpid::broker::RecoveryManager& recoverer,
                                  ExchangeMap& exchangeMap);
    virtual void recoverQueues(qpid::broker::RecoveryManager& recoverer,
                               QueueMap& queueMap);
    virtual void recoverBindings(qpid::broker::RecoveryManager& recoverer,
                                 const ExchangeMap& exchangeMap,
                                 const QueueMap& queueMap);
    virtual void recoverMessages(qpid::broker::RecoveryManager& recoverer,
                                 MessageMap& messageMap,
                                 MessageQueueMap& messageQueueMap);
    virtual void recoverTransactions(qpid::broker::RecoveryManager& recoverer,
                                     PreparedTransactionMap& dtxMap);

private:    // helpers
    bool create(const std::string& table, IdSequence& seq, const qpid::broker::Persistable& p);
    void destroy(const std::string& table, const qpid::broker::Persistable& p);

    template <typename RecoverableType, typename TypeMap =
        std::map<uint64_t, typename RecoverableType:: shared_ptr>>
    void recover_helper(const std::string &table, IdSequence& seq,
                        qpid::broker::RecoveryManager& recoverer, TypeMap* internal_map = 0);

    void deleteBindingsForQueue(const qpid::broker::PersistableQueue& queue);
    void deleteBindingsForExchange(const qpid::broker::PersistableExchange& exchange);

private:
    struct ProviderOptions : public qpid::Options
    {
        std::string databaseName;

        ProviderOptions(const std::string& name)
            : qpid::Options(name),
              databaseName("apache_qpid")
        {
            // const enum { NAMELEN = MAX_COMPUTERNAME_LENGTH + 1 };
            // TCHAR myName[NAMELEN];
            // DWORD myNameLen = NAMELEN;
            // GetComputerName(myName, &myNameLen);
            // connectString = "Data Source=";
            // connectString += myName;
            // connectString += "\\SQLEXPRESS;Integrated Security=SSPI";
            // addOptions()
            //     ("connect",
            //      qpid::optValue(connectString, "STRING"),
            //      "Connection string for the database to use. Will prepend "
            //      "Provider=SQLOLEDB;")
            //     ("catalog",
            //      qpid::optValue(catalogName, "DB NAME"),
            //      "Catalog (database) name")
            //     ;
        }
    };
    ProviderOptions options;

    IdSequence queueIdSequence;
    IdSequence exchangeIdSequence;
    IdSequence configIdSequence;
    IdSequence messageIdSequence;

    // Each thread has a separate connection to the database and also needs
    // to manage its COM initialize/finalize individually. This is done by
    // keeping a thread-specific State.
    boost::thread_specific_ptr<R::Connection> connections;

    R::Connection* initConnection(void);
    void createDb(R::Connection *conn, const std::string& name);
};

static RethinkDBProvider static_instance_registers_plugin;

void RethinkDBProvider::finalizeMe()
{
    QPID_LOG(notice, "RethinkDBProvider::finalizeMe");
    connections.reset();
}

RethinkDBProvider::RethinkDBProvider()
    : options("RethinkDB Provider options")
{
}

RethinkDBProvider::~RethinkDBProvider()
{
}

void RethinkDBProvider::earlyInitialize(Plugin::Target& target)
{
    QPID_LOG(notice, "RethinkDBProvider::earlyInitialize");
    MessageStorePlugin *store = dynamic_cast<MessageStorePlugin *>(&target);
    if (!store) {
        QPID_LOG(notice, "RethinkDBProvider::earlyInitialize invalid store pointer");
        return;
    }

    try {
        std::unique_ptr<R::Connection> conn = R::connect();  // @TODO: use settings
        bool database_exists = R::db_list()
            .contains(options.databaseName)
            .run(*conn)
            .to_datum()
            .extract_boolean();

        if (!database_exists) {
            QPID_LOG(notice, "RethinkDB: creating database " + options.databaseName);
            createDb(conn.get(), options.databaseName);
        } else {
            QPID_LOG(notice, "RethinkDB: database located: " + options.databaseName);
        }

        conn->close();
        store->providerAvailable("RethinkDB", this);
    } catch (qpid::Exception& e) {
        QPID_LOG(error, e.what());
        return;
    } catch (const R::Error& e) {
        QPID_LOG(error, e.message);
        return;
    }

    store->addFinalizer(boost::bind(&RethinkDBProvider::finalizeMe, this));
}

void RethinkDBProvider::initialize(Plugin::Target& /*target*/)
{ }

void RethinkDBProvider::activate(MessageStorePlugin& /*store*/)
{
    QPID_LOG(info, "RethinkDB Provider is up");
}

bool RethinkDBProvider::create(const std::string& table, IdSequence& seq,
                               const qpid::broker::Persistable& p)
{
    uint64_t primary_key(seq.next());
    std::vector<char> data(p.encodedSize());
    qpid::framing::Buffer blob(data.data(), data.size());
    p.encode(blob);

    try {
        R::Connection* conn = initConnection();
        R::db(options.databaseName).table(table)
            .insert(R::Object{
                { "id", primary_key },
                { "blob", R::Binary(std::string(data.data(), data.size())) }
            })
            .run(*conn);
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::create exception: " + e.message);
        throw e;
    }

    // @TODO:
    //  - maybe add a conflict strategy
    //  - return `false` if key already exists; don't set persistence id in this case'
    //  - cleanup if there is any sort of error

    p.setPersistenceId(primary_key);
    return true;
}

void RethinkDBProvider::deleteBindingsForQueue(const qpid::broker::PersistableQueue& queue)
{
    try {
        R::Connection* conn = initConnection();
        R::db(options.databaseName).table(TblBinding)
            .filter(R::Object{ { "queue_id", queue.getPersistenceId() } })
            .delete_()
            .run(*conn);
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::deleteBindingsForQueue exception: " + e.message);
        throw e;
    }
}

void RethinkDBProvider::deleteBindingsForExchange(const qpid::broker::PersistableExchange& exchange)
{
    try {
        R::Connection* conn = initConnection();
        R::db(options.databaseName).table(TblBinding)
            .filter(R::Object{ { "exchange_id", exchange.getPersistenceId() } })
            .delete_()
            .run(*conn);
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::deleteBindingsForQueue exception: " + e.message);
        throw e;
    }
}

void RethinkDBProvider::destroy(const std::string& table, const qpid::broker::Persistable& p)
{
    try {
        R::Connection* conn = initConnection();
        R::db(options.databaseName).table(table)
            .get(p.getPersistenceId())
            .delete_()
            .run(*conn);
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::destroy exception: " + e.message);
        throw e;
    }
}

void RethinkDBProvider::create(PersistableQueue& queue,
                               const qpid::framing::FieldTable& /*args*/)
{
    QPID_LOG(notice, "RethinkDBProvider::create queue=" + queue.getName());
    if (queue.getPersistenceId()) {
        THROW_RDB_EXCEPTION("Queue already created: " + queue.getName());
    }

    try {
        if (!create(TblQueue, queueIdSequence, queue)) {
            THROW_RDB_EXCEPTION("Queue already exists: " + queue.getName());
        }
    } catch (const R::Error& e) {
        THROW_RDB_EXCEPTION_2("Error creating queue named " + queue.getName(), e);
    }
}

template <typename RecoverableType>
RecoverableType recover_type(
    qpid::broker::RecoveryManager& recoverer, qpid::framing::Buffer& blob)
{
    return 0;
}

template <>
qpid::broker::RecoverableExchange::shared_ptr recover_type(
    qpid::broker::RecoveryManager& recoverer, qpid::framing::Buffer& blob)
{
    return recoverer.recoverExchange(blob);
}

template <>
qpid::broker::RecoverableQueue::shared_ptr recover_type(
    qpid::broker::RecoveryManager& recoverer, qpid::framing::Buffer& blob)
{
    return recoverer.recoverQueue(blob);
}

template <>
qpid::broker::RecoverableConfig::shared_ptr recover_type(
    qpid::broker::RecoveryManager& recoverer, qpid::framing::Buffer& blob)
{
    return recoverer.recoverConfig(blob);
}

template <typename RecoverableType, typename TypeMap>
void RethinkDBProvider::recover_helper(const std::string &table, IdSequence& seq,
                                       qpid::broker::RecoveryManager& recoverer,
                                       TypeMap* internal_map)
{
    try {
        uint64_t max_id = 0;
        R::Connection* conn = initConnection();
        R::Cursor cursor = R::db(options.databaseName).table(table).run(*conn);
        while (cursor.has_next()) {
            R::Datum type_data = cursor.next();
            uint64_t primary_key =
                static_cast<uint64_t>(type_data.extract_field("id").extract_number());
            R::Binary blob_data = type_data.extract_field("blob").extract_binary();
            qpid::framing::Buffer blob(
                const_cast<char*>(blob_data.data.data()), (uint32_t)blob_data.data.size());

            typedef typename RecoverableType::shared_ptr recoverable_shared_ptr;
            recoverable_shared_ptr object =
                recover_type<recoverable_shared_ptr>(recoverer, blob);

            object->setPersistenceId(primary_key);
            if (internal_map) {
                (*internal_map)[primary_key] = object;
            }

            fprintf(stderr, "   recovered object(id=%lu)\n", primary_key);
            max_id = std::max(max_id, primary_key);
        }

        // start sequence generation one after highest id we recovered
        seq.reset(max_id + 1);
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::recover exception: " + e.message);
        throw e;
    }
}

/**
 * Destroy a durable queue
 */
void RethinkDBProvider::destroy(PersistableQueue& queue)
{
    QPID_LOG(notice, "RethinkDBProvider::destroy queue=" + queue.getName());
    deleteBindingsForQueue(queue);
    destroy(TblQueue, queue);
    // @TODO: delete all messages associated with queue
}

/**
 * Record the existence of a durable exchange
 */
void RethinkDBProvider::create(const PersistableExchange& exchange,
                               const qpid::framing::FieldTable& /*args*/)
{
    QPID_LOG(notice, "RethinkDBProvider::create exchange=" + exchange.getName());
    if (exchange.getPersistenceId()) {
        THROW_RDB_EXCEPTION("Exchange already created: " + exchange.getName());
    }

    try {
        if (!create(TblExchange, exchangeIdSequence, exchange)) {
            THROW_RDB_EXCEPTION("Exchange already exists: " + exchange.getName());
        }
    } catch (const R::Error& e) {
        THROW_RDB_EXCEPTION_2("Error creating exchange named " + exchange.getName(), e);
    }
}

/**
 * Destroy a durable exchange
 */
void RethinkDBProvider::destroy(const PersistableExchange& exchange)
{
    QPID_LOG(notice, "RethinkDBProvider::destroy exchange=" + exchange.getName());
    deleteBindingsForExchange(exchange);
    destroy(TblExchange, exchange);
}

/**
 * Record a binding
 */
void RethinkDBProvider::bind(const PersistableExchange& exchange,
                             const PersistableQueue& queue,
                             const std::string& key,
                             const qpid::framing::FieldTable& args)
{
    QPID_LOG(notice, "RethinkDBProvider::bind");

    std::vector<char> args_data(args.encodedSize());
    qpid::framing::Buffer args_blob(args_data.data(), args_data.size());
    args.encode(args_blob);

    try {
        R::Connection* conn = initConnection();

        // @TODO: optimize this later if its important, perhaps use a compound
        //        index of an array of all four values?
        R::db(options.databaseName).table(TblBinding)
            .insert(R::Object{
                { "exchange_id", exchange.getPersistenceId() },
                { "queue_id", queue.getPersistenceId() },
                { "routing_key", key },
                { "args", R::Binary(std::string(args_data.data(), args_data.size())) }
            })
            .run(*conn);
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::bind exception: " + e.message);
        throw e;
    }
}

/**
 * Forget a binding
 */
void RethinkDBProvider::unbind(const PersistableExchange& exchange,
                               const PersistableQueue& queue,
                               const std::string& key,
                               const qpid::framing::FieldTable& args)
{
    QPID_LOG(notice, "RethinkDBProvider::unbind");

    std::vector<char> args_data(args.encodedSize());
    qpid::framing::Buffer args_blob(args_data.data(), args_data.size());
    args.encode(args_blob);

    try {
        R::Connection* conn = initConnection();

        // @TODO: optimize this later if its important. Perhaps use a compound
        //        index of an array of all four values?
        R::db(options.databaseName).table(TblBinding)
            .filter(R::Object{
                { "exchange_id", exchange.getPersistenceId() },
                { "queue_id", queue.getPersistenceId() },
                { "routing_key", key },
                { "args", R::Binary(std::string(args_data.data(), args_data.size())) }
            })
            .delete_()
            .run(*conn);
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::unbind exception: " + e.message);
        throw e;
    }
}

/**
 * Record generic durable configuration
 */
void RethinkDBProvider::create(const PersistableConfig& config)
{
    QPID_LOG(notice, "RethinkDBProvider::create config=" + config.getName());
    if (config.getPersistenceId()) {
        THROW_RDB_EXCEPTION("Config already created: " + config.getName());
    }

    try {
        if (!create(TblConfig, configIdSequence, config)) {
            THROW_RDB_EXCEPTION("Config already exists: " + config.getName());
        }
    } catch (const R::Error& e) {
        THROW_RDB_EXCEPTION_2("Error creating config named " + config.getName(), e);
    }
}

/**
 * Enqueues a message, storing the message if it has not
 * been previously stored and recording that the given
 * message is on the given queue.
 *
 * @param ctxt The transaction context under which this enqueue happens.
 * @param msg The message to enqueue
 * @param queue the name of the queue onto which it is to be enqueued
 */
void RethinkDBProvider::enqueue(qpid::broker::TransactionContext* ctxt,
                                const boost::intrusive_ptr<PersistableMessage>& msg,
                                const PersistableQueue& queue)
{
    QPID_LOG(notice, "RethinkDBProvider::enqueue");
    if (ctxt) {
        THROW_RDB_EXCEPTION("Transactions are not presently implemented");
    }

    uint64_t queue_id(queue.getPersistenceId());
    if (queue_id == 0) {
        THROW_RDB_EXCEPTION("Queue not created: " + queue.getName());
    }

    if (msg->getPersistenceId() == 0) {
        msg->setPersistenceId(messageIdSequence.next());
    }

    std::vector<char> msg_data(msg->encodedSize() + sizeof(uint32_t));
    qpid::framing::Buffer msg_blob(msg_data.data(), msg_data.size());
    msg_blob.putLong(msg->encodedHeaderSize());
    msg->encode(msg_blob);

    try {
        R::Connection* conn = initConnection();
        R::db(options.databaseName).table(TblMessage)
            .insert(R::Object{
                { "id", R::Array{ queue_id, msg->getPersistenceId() } },
                { "blob", R::Binary(std::string(msg_data.data(), msg_data.size())) }
            })
            .run(*conn);
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::enqueue exception: " + e.message);
        throw e;
    }

    msg->enqueueComplete();
}

/**
 * Dequeues a message, recording that the given message is
 * no longer on the given queue and deleting the message
 * if it is no longer on any other queue.
 *
 * @param ctxt The transaction context under which this dequeue happens.
 * @param msg The message to dequeue
 * @param queue The queue from which it is to be dequeued
 */
void RethinkDBProvider::dequeue(qpid::broker::TransactionContext* ctxt,
                                const boost::intrusive_ptr<PersistableMessage>& msg,
                                const PersistableQueue& queue)
{
    QPID_LOG(notice, "RethinkDBProvider::dequeue");

    if (ctxt) {
        THROW_RDB_EXCEPTION("Transactions are not presently implemented");
    }

    uint64_t queue_id(queue.getPersistenceId());
    if (queue_id == 0) {
        THROW_RDB_EXCEPTION("Queue \"" + queue.getName() + "\" has null queue Id (has not been created)");
    }

    uint64_t message_id(msg->getPersistenceId());
    if (message_id == 0) {
        THROW_RDB_EXCEPTION("Queue \"" + queue.getName() + "\": Dequeuing message with null persistence id.");
    }

    try {
        R::Connection* conn = initConnection();
        R::db(options.databaseName).table(TblMessage)
            .get(R::Array{ queue_id, message_id })
            .delete_()
            .run(*conn);
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::enqueue exception: " + e.message);
        throw e;
    }

    msg->dequeueComplete();
}

std::auto_ptr<qpid::broker::TransactionContext> RethinkDBProvider::begin()
{
    QPID_LOG(notice, "RethinkDBProvider::begin");
    THROW_RDB_EXCEPTION("Not implemented");
    // return std::auto_ptr<qpid::broker::TransactionContext>();
}

std::auto_ptr<qpid::broker::TPCTransactionContext>
RethinkDBProvider::begin(const std::string& /*xid*/)
{
    QPID_LOG(notice, "RethinkDBProvider::begin");
    THROW_RDB_EXCEPTION("Not implemented");
    // return std::auto_ptr<qpid::broker::TPCTransactionContext>();
}

void RethinkDBProvider::prepare(qpid::broker::TPCTransactionContext& /*txn*/)
{
    QPID_LOG(notice, "RethinkDBProvider::prepare");
    THROW_RDB_EXCEPTION("Not implemented");
}

void RethinkDBProvider::commit(qpid::broker::TransactionContext& /*txn*/)
{
    QPID_LOG(notice, "RethinkDBProvider::commit");
    THROW_RDB_EXCEPTION("Not implemented");
}

void RethinkDBProvider::abort(qpid::broker::TransactionContext& /*txn*/)
{
    QPID_LOG(notice, "RethinkDBProvider::abort");
    THROW_RDB_EXCEPTION("Not implemented");
}

void RethinkDBProvider::collectPreparedXids(std::set<std::string>& /*xids*/)
{
    QPID_LOG(notice, "RethinkDBProvider::collectPreparedXids");
    THROW_RDB_EXCEPTION("Not implemented");
}

void RethinkDBProvider::recoverConfigs(qpid::broker::RecoveryManager& recoverer)
{
    QPID_LOG(notice, "RethinkDBProvider::recoverConfigs");
    recover_helper<broker::RecoverableConfig>(TblConfig, configIdSequence, recoverer);
}

void RethinkDBProvider::recoverExchanges(qpid::broker::RecoveryManager& recoverer,
                                         ExchangeMap& exchangeMap)
{
    QPID_LOG(notice, "RethinkDBProvider::recoverExchanges");
    recover_helper<broker::RecoverableExchange, ExchangeMap>(
        TblExchange, exchangeIdSequence, recoverer, &exchangeMap);
}

void RethinkDBProvider::recoverQueues(qpid::broker::RecoveryManager& recoverer,
                                      QueueMap& queueMap)
{
    QPID_LOG(notice, "RethinkDBProvider::recoverQueues");
    recover_helper<broker::RecoverableQueue, QueueMap>(
        TblQueue, queueIdSequence, recoverer, &queueMap);
}

void RethinkDBProvider::recoverBindings(qpid::broker::RecoveryManager& /*recoverer*/,
                                        const ExchangeMap& exchangeMap,
                                        const QueueMap& queueMap)
{
    QPID_LOG(notice, "RethinkDBProvider::recoverBindings");

    try {
        R::Connection* conn = initConnection();
        R::Cursor cursor = R::db(options.databaseName).table(TblBinding).run(*conn);
        while (cursor.has_next()) {
            R::Datum binding_data = cursor.next();
            uint64_t exchange_id(binding_data.extract_field("exchange_id").extract_number());
            uint64_t queue_id(binding_data.extract_field("queue_id").extract_number());
            std::string routing_key(binding_data.extract_field("routing_key").extract_string());
            R::Binary args_data = binding_data.extract_field("args").extract_binary();
            qpid::framing::Buffer args_blob(
                const_cast<char*>(args_data.data.data()), (uint32_t)args_data.data.size());
            qpid::framing::FieldTable args;
            args_blob.get(args);

            store::ExchangeMap::const_iterator exchange = exchangeMap.find(exchange_id);
            store::QueueMap::const_iterator queue = queueMap.find(queue_id);
            if (exchange != exchangeMap.end() && queue != queueMap.end()) {
                broker::RecoverableExchange::shared_ptr exchangePtr = exchange->second;
                broker::RecoverableQueue::shared_ptr queuePtr = queue->second;
                exchangePtr->bind(queuePtr->getName(), routing_key, args);
                QPID_LOG(info, "Recovered binding exchange=" + exchangePtr->getName()
                    + " key=" + routing_key
                    + " queue=" + queuePtr->getName());
            } else {
                QPID_LOG(warning, "Deleting stale binding");
                R::db(options.databaseName).table(TblBinding)
                    .get(binding_data.extract_field("id").extract_string())
                    .delete_()
                    .run(*conn);
            }
        }
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::create exception: " + e.message);
        throw e;
    }
}

void RethinkDBProvider::recoverMessages(qpid::broker::RecoveryManager& recoverer,
                                        MessageMap& messageMap,
                                        MessageQueueMap& messageQueueMap)
{
    QPID_LOG(notice, "RethinkDBProvider::recoverMessages");

    size_t preamble_length = sizeof(uint32_t);  // header size

    try {
        R::Connection* conn = initConnection();
        R::Cursor cursor = R::db(options.databaseName).table(TblMessage).run(*conn);
        while (cursor.has_next()) {
            R::Datum message_data = cursor.next();
            uint64_t queue_id(message_data.extract_field("id").extract_nth(0).extract_number());
            uint64_t message_id(message_data.extract_field("id").extract_nth(1).extract_number());
            R::Binary blob_data = message_data.extract_field("blob").extract_binary();

            // @TODO: support transactions

            // handle message map
            qpid::store::QueueEntry queue_entry(queue_id);
            queue_entry.tplStatus = qpid::store::QueueEntry::NONE;
            messageQueueMap[message_id].push_back(queue_entry);

            // handle message queue map
            uint32_t header_size = qpid::framing::Buffer(
                const_cast<char*>(blob_data.data.data()), preamble_length).getLong();
            qpid::framing::Buffer message_blob(
                const_cast<char*>(blob_data.data.data()) + preamble_length, header_size);
            broker::RecoverableMessage::shared_ptr msg =
                recoverer.recoverMessage(message_blob);
            msg->setPersistenceId(message_id);

            // At some future point if delivery attempts are stored, then this call would
            // become optional depending on that information.
            msg->setRedelivered();
            // Reset the TTL for the recovered message
            msg->computeExpiration();
            messageMap[message_id] = msg;
        }
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::create exception: " + e.message);
        throw e;
    }
}

void RethinkDBProvider::recoverTransactions(qpid::broker::RecoveryManager& /*recoverer*/,
                                            PreparedTransactionMap& /*dtxMap*/)
{
    QPID_LOG(notice, "RethinkDBProvider::recoverTransactions");
    // THROW_RDB_EXCEPTION("Not implemented");
}

/**
 * Destroy generic durable configuration
 */
void RethinkDBProvider::destroy(const PersistableConfig& config)
{
    QPID_LOG(notice, "RethinkDBProvider::destroy config=" + config.getName());
    destroy(TblConfig, config);
}

void RethinkDBProvider::stage(const boost::intrusive_ptr<PersistableMessage>& /*msg*/)
{
    THROW_RDB_EXCEPTION("Not implemented");
}

void RethinkDBProvider::destroy(PersistableMessage& /*msg*/)
{
    THROW_RDB_EXCEPTION("Not implemented");
}

void RethinkDBProvider::appendContent(const boost::intrusive_ptr<const PersistableMessage>& /*msg*/,
                                      const std::string& /*data*/)
{
    THROW_RDB_EXCEPTION("Not implemented");
}

void RethinkDBProvider::loadContent(const qpid::broker::PersistableQueue& /*queue*/,
                                    const boost::intrusive_ptr<const PersistableMessage>& /*msg*/,
                                    std::string& /*data*/,
                                    uint64_t /*offset*/,
                                    uint32_t /*length*/)
{
    THROW_RDB_EXCEPTION("Not implemented");
}

////////////// Internal Methods

R::Connection* RethinkDBProvider::initConnection(void)
{
    R::Connection* conn = connections.get();
    if (!conn) {
        std::unique_ptr<R::Connection> conn_ = R::connect();
        conn = conn_.release();
        connections.reset(conn);
    }

    return conn;
}

void RethinkDBProvider::createDb(R::Connection *conn, const std::string& name)
{
    try {
        R::db_create(name).run(*conn);
        R::db(name).table_create(TblQueue).run(*conn);
        R::db(name).table_create(TblExchange).run(*conn);
        R::db(name).table_create(TblConfig).run(*conn);
        R::db(name).table_create(TblMessage).run(*conn);
        R::db(name).table_create(TblBinding).run(*conn);
        R::db(name).table_create(TblTpl).run(*conn);
        R::db(name).table_create(TblMessageMap).run(*conn);
    } catch (const R::Error& error) {
        QPID_LOG(notice, "something went horribly wrong: " + error.message);
    }
}

void RethinkDBProvider::dump()
{
  // dump all db records to qpid_log
  QPID_LOG(notice, "DB Dump: (not dumping anything)");
}

}}} // namespace qpid::store::rethinkdb
