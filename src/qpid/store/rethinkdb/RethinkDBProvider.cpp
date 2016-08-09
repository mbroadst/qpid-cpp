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

/*
#include "AmqpTransaction.h"
#include "BlobAdapter.h"
#include "BlobRecordset.h"
#include "BindingRecordset.h"
#include "MessageMapRecordset.h"
#include "MessageRecordset.h"
#include "TplRecordset.h"
#include "DatabaseConnection.h"
#include "Exception.h"
#include "State.h"
#include "VariantHelper.h"

// Bring in ADO 2.8 (yes, I know it says "15", but that's it...)
#import "C:\Program Files\Common Files\System\ado\msado15.dll" \
        no_namespace rename("EOF", "EndOfFile")
#include <comdef.h>

namespace {
inline void TESTHR(HRESULT x) {if FAILED(x) _com_issue_error(x);};

}
*/

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

class IdSequence
{
    std::mutex lock;
    uint64_t id;
public:
    IdSequence() : id(1) {}
    uint64_t next() {
        std::unique_lock<std::mutex> guard(lock);
        if (!id) id++; // avoid 0 when folding around
        return id++;
    }

    void reset(uint64_t value) {
        // deliberately not threadsafe, used only on recovery
        id = value;
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

/**
 * Destroy a durable queue
 */
void RethinkDBProvider::destroy(PersistableQueue& queue)
{
    QPID_LOG(notice, "RethinkDBProvider::destroy queue=" + queue.getName());

/*
    DatabaseConnection *db = initConnection();
    BlobRecordset rsQueues;
    BindingRecordset rsBindings;
    MessageRecordset rsMessages;
    MessageMapRecordset rsMessageMaps;
    try {
        db->beginTransaction();
        rsQueues.open(db, TblQueue);
        rsBindings.open(db, TblBinding);
        rsMessages.open(db, TblMessage);
        rsMessageMaps.open(db, TblMessageMap);
        // Remove bindings first; the queue IDs can't be ripped out from
        // under the references in the bindings table. Then remove the
        // message->queue entries for the queue, also because the queue can't
        // be deleted while there are references to it. If there are messages
        // orphaned by removing the queue references, they're deleted by
        // a trigger on the tblMessageMap table. Lastly, the queue record
        // can be removed.
        rsBindings.removeForQueue(queue.getPersistenceId());
        rsMessageMaps.removeForQueue(queue.getPersistenceId());
        rsQueues.remove(queue);
        db->commitTransaction();
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        db->rollbackTransaction();
        throw ADOException("Error deleting queue " + queue.getName(), e, errs);
    }
*/
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

/*
    DatabaseConnection *db = initConnection();
    BlobRecordset rsExchanges;
    BindingRecordset rsBindings;
    try {
        db->beginTransaction();
        rsExchanges.open(db, TblExchange);
        rsBindings.open(db, TblBinding);
        // Remove bindings first; the exchange IDs can't be ripped out from
        // under the references in the bindings table.
        rsBindings.removeForExchange(exchange.getPersistenceId());
        rsExchanges.remove(exchange);
        db->commitTransaction();
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        db->rollbackTransaction();
        throw ADOException("Error deleting exchange " + exchange.getName(),
                           e,
                           errs);
    }
*/
}

/**
 * Record a binding
 */
void RethinkDBProvider::bind(const PersistableExchange& /*exchange*/,
                             const PersistableQueue& /*queue*/,
                             const std::string& /*key*/,
                             const qpid::framing::FieldTable& /*args*/)
{
    QPID_LOG(notice, "RethinkDBProvider::bind");

/*
    DatabaseConnection *db = initConnection();
    BindingRecordset rsBindings;
    try {
        db->beginTransaction();
        rsBindings.open(db, TblBinding);
        rsBindings.add(exchange.getPersistenceId(),
                       queue.getPersistenceId(),
                       key,
                       args);
        db->commitTransaction();
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        db->rollbackTransaction();
        throw ADOException("Error binding exchange " + exchange.getName() +
                           " to queue " + queue.getName(),
                           e,
                           errs);
    }
*/
}

/**
 * Forget a binding
 */
void RethinkDBProvider::unbind(const PersistableExchange& /*exchange*/,
                               const PersistableQueue& /*queue*/,
                               const std::string& /*key*/,
                               const qpid::framing::FieldTable& /*args*/)
{
    QPID_LOG(notice, "RethinkDBProvider::unbind");

/*
    DatabaseConnection *db = initConnection();
    BindingRecordset rsBindings;
    try {
        db->beginTransaction();
        rsBindings.open(db, TblBinding);
        rsBindings.remove(exchange.getPersistenceId(),
                          queue.getPersistenceId(),
                          key,
                          args);
        db->commitTransaction();
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        db->rollbackTransaction();
        throw ADOException("Error unbinding exchange " + exchange.getName() +
                           " from queue " + queue.getName(),
                           e,
                           errs);
    }
*/
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
 * Destroy generic durable configuration
 */
void RethinkDBProvider::destroy(const PersistableConfig& /*config*/)
{
    QPID_LOG(notice, "RethinkDBProvider::stage");

/*
    DatabaseConnection *db = initConnection();
    BlobRecordset rsConfigs;
    try {
        db->beginTransaction();
        rsConfigs.open(db, TblConfig);
        rsConfigs.remove(config);
        db->commitTransaction();
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        db->rollbackTransaction();
        throw ADOException("Error deleting config " + config.getName(), e, errs);
    }
*/
}

/**
 * Stores a messages before it has been enqueued
 * (enqueueing automatically stores the message so this is
 * only required if storage is required prior to that
 * point). If the message has not yet been stored it will
 * store the headers as well as any content passed in. A
 * persistence id will be set on the message which can be
 * used to load the content or to append to it.
 */
void RethinkDBProvider::stage(const boost::intrusive_ptr<PersistableMessage>& /*msg*/)
{
    QPID_LOG(notice, "RethinkDBProvider::stage");

/*
    DatabaseConnection *db = initConnection();
    MessageRecordset rsMessages;
    try {
        db->beginTransaction();
        rsMessages.open(db, TblMessage);
        rsMessages.add(msg);
        db->commitTransaction();
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        db->rollbackTransaction();
        throw ADOException("Error staging message", e, errs);
    }
*/
}

/**
 * Destroys a previously staged message. This only needs
 * to be called if the message is never enqueued. (Once
 * enqueued, deletion will be automatic when the message
 * is dequeued from all queues it was enqueued onto).
 */
void RethinkDBProvider::destroy(PersistableMessage& /*msg*/)
{
    QPID_LOG(notice, "RethinkDBProvider::destroy");

/*
    DatabaseConnection *db = initConnection();
    BlobRecordset rsMessages;
    try {
        db->beginTransaction();
        rsMessages.open(db, TblMessage);
        rsMessages.remove(msg);
        db->commitTransaction();
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        db->rollbackTransaction();
        throw ADOException("Error deleting message", e, errs);
    }
*/
}

/**
 * Appends content to a previously staged message
 */
void RethinkDBProvider::appendContent(const boost::intrusive_ptr<const PersistableMessage>& /*msg*/,
                                      const std::string& /*data*/)
{
    QPID_LOG(notice, "RethinkDBProvider::appendContent");

/*
    DatabaseConnection *db = initConnection();
    MessageRecordset rsMessages;
    try {
        db->beginTransaction();
        rsMessages.open(db, TblMessage);
        rsMessages.append(msg, data);
        db->commitTransaction();
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        db->rollbackTransaction();
        throw ADOException("Error appending to message", e, errs);
    }
*/
}

/**
 * Loads (a section) of content data for the specified
 * message (previously stored through a call to stage or
 * enqueue) into data. The offset refers to the content
 * only (i.e. an offset of 0 implies that the start of the
 * content should be loaded, not the headers or related
 * meta-data).
 */
void RethinkDBProvider::loadContent(const qpid::broker::PersistableQueue& /*queue*/,
                                    const boost::intrusive_ptr<const PersistableMessage>& /*msg*/,
                                    std::string& /*data*/,
                                    uint64_t /*offset*/,
                                    uint32_t /*length*/)
{
    QPID_LOG(notice, "RethinkDBProvider::loadContent");

/*
    // SQL store keeps all messages in one table, so we don't need the
    // queue reference.
    DatabaseConnection *db = initConnection();
    MessageRecordset rsMessages;
    try {
        rsMessages.open(db, TblMessage);
        rsMessages.loadContent(msg, data, offset, length);
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        throw ADOException("Error loading message content", e, errs);
    }
*/
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
void RethinkDBProvider::enqueue(qpid::broker::TransactionContext* /*ctxt*/,
                                const boost::intrusive_ptr<PersistableMessage>& /*msg*/,
                                const PersistableQueue& /*queue*/)
{
    QPID_LOG(notice, "RethinkDBProvider::enqueue");

/*
    // If this enqueue is in the context of a transaction, use the specified
    // transaction to nest a new transaction for this operation. However, if
    // this is not in the context of a transaction, then just use the thread's
    // DatabaseConnection with a ADO transaction.
    DatabaseConnection *db = 0;
    std::string xid;
    AmqpTransaction *atxn = dynamic_cast<AmqpTransaction*> (ctxt);
    if (atxn == 0) {
        db = initConnection();
        db->beginTransaction();
    }
    else {
        (void)initState();     // Ensure this thread is initialized
        // It's a transactional enqueue; if it's TPC, grab the xid.
        AmqpTPCTransaction *tpcTxn = dynamic_cast<AmqpTPCTransaction*> (ctxt);
        if (tpcTxn)
            xid = tpcTxn->getXid();
        db = atxn->dbConn();
        try {
            atxn->sqlBegin();
        }
        catch(_com_error &e) {
            throw ADOException("Error queuing message", e, db->getErrors());
        }
    }

    MessageRecordset rsMessages;
    MessageMapRecordset rsMap;
    try {
        if (msg->getPersistenceId() == 0) {    // Message itself not yet saved
            rsMessages.open(db, TblMessage);
            rsMessages.add(msg);
        }
        rsMap.open(db, TblMessageMap);
        rsMap.add(msg->getPersistenceId(), queue.getPersistenceId(), xid);
        if (atxn)
            atxn->sqlCommit();
        else
            db->commitTransaction();
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        if (atxn)
            atxn->sqlAbort();
        else
            db->rollbackTransaction();
        throw ADOException("Error queuing message", e, errs);
    }
    msg->enqueueComplete();
*/
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
void RethinkDBProvider::dequeue(qpid::broker::TransactionContext* /*ctxt*/,
                                const boost::intrusive_ptr<PersistableMessage>& /*msg*/,
                                const PersistableQueue& /*queue*/)
{
    QPID_LOG(notice, "RethinkDBProvider::dequeue");

/*
    // If this dequeue is in the context of a transaction, use the specified
    // transaction to nest a new transaction for this operation. However, if
    // this is not in the context of a transaction, then just use the thread's
    // DatabaseConnection with a ADO transaction.
    DatabaseConnection *db = 0;
    std::string xid;
    AmqpTransaction *atxn = dynamic_cast<AmqpTransaction*> (ctxt);
    if (atxn == 0) {
        db = initConnection();
        db->beginTransaction();
    }
    else {
        (void)initState();     // Ensure this thread is initialized
        // It's a transactional dequeue; if it's TPC, grab the xid.
        AmqpTPCTransaction *tpcTxn = dynamic_cast<AmqpTPCTransaction*> (ctxt);
        if (tpcTxn)
            xid = tpcTxn->getXid();
        db = atxn->dbConn();
        try {
            atxn->sqlBegin();
        }
        catch(_com_error &e) {
            throw ADOException("Error queuing message", e, db->getErrors());
        }
    }

    MessageMapRecordset rsMap;
    try {
        rsMap.open(db, TblMessageMap);
        // TPC dequeues are just marked pending and will actually be removed
        // when the transaction commits; Single-phase dequeues are removed
        // now, relying on the SQL transaction to put it back if the
        // transaction doesn't commit.
        if (!xid.empty()) {
            rsMap.pendingRemove(msg->getPersistenceId(),
                                queue.getPersistenceId(),
                                xid);
        }
        else {
            rsMap.remove(msg->getPersistenceId(),
                         queue.getPersistenceId());
        }
        if (atxn)
            atxn->sqlCommit();
        else
            db->commitTransaction();
    }
    catch(ms_sql::Exception&) {
        if (atxn)
            atxn->sqlAbort();
        else
            db->rollbackTransaction();
        throw;
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        if (atxn)
            atxn->sqlAbort();
        else
            db->rollbackTransaction();
        throw ADOException("Error dequeuing message", e, errs);
    }
    msg->dequeueComplete();
*/
}

std::auto_ptr<qpid::broker::TransactionContext> RethinkDBProvider::begin()
{
    QPID_LOG(notice, "RethinkDBProvider::begin");
    return std::auto_ptr<qpid::broker::TransactionContext>();

/*
    (void)initState();     // Ensure this thread is initialized

    // Transactions are associated with the Connection, so this transaction
    // context needs its own connection. At the time of writing, single-phase
    // transactions are dealt with completely on one thread, so we really
    // could just use the thread-specific DatabaseConnection for this.
    // However, that would introduce an ugly, hidden coupling, so play
    // it safe and handle this just like a TPC transaction, which actually
    // can be prepared and committed/aborted from different threads,
    // making it a bad idea to try using the thread-local DatabaseConnection.
    boost::shared_ptr<DatabaseConnection> db(new DatabaseConnection);
    db->open(options.connectString, options.catalogName);
    std::auto_ptr<AmqpTransaction> tx(new AmqpTransaction(db));
    tx->sqlBegin();
    std::auto_ptr<qpid::broker::TransactionContext> tc(tx);
    return tc;
*/
}

std::auto_ptr<qpid::broker::TPCTransactionContext>
RethinkDBProvider::begin(const std::string& /*xid*/)
{
    QPID_LOG(notice, "RethinkDBProvider::begin");
    return std::auto_ptr<qpid::broker::TPCTransactionContext>();

/*
    (void)initState();     // Ensure this thread is initialized
    boost::shared_ptr<DatabaseConnection> db(new DatabaseConnection);
    db->open(options.connectString, options.catalogName);
    std::auto_ptr<AmqpTPCTransaction> tx(new AmqpTPCTransaction(db, xid));
    tx->sqlBegin();

    TplRecordset rsTpl;
    try {
        tx->sqlBegin();
        rsTpl.open(db.get(), TblTpl);
        rsTpl.add(xid);
        tx->sqlCommit();
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        tx->sqlAbort();
        throw ADOException("Error adding TPL record", e, errs);
    }

    std::auto_ptr<qpid::broker::TPCTransactionContext> tc(tx);
    return tc;
*/
}

void RethinkDBProvider::prepare(qpid::broker::TPCTransactionContext& /*txn*/)
{
    QPID_LOG(notice, "RethinkDBProvider::prepare");

/*
    // Commit all the marked-up enqueue/dequeue ops and the TPL record.
    // On commit/rollback the TPL will be removed and the TPL markups
    // on the message map will be cleaned up as well.
    (void)initState();     // Ensure this thread is initialized
    AmqpTPCTransaction *atxn = dynamic_cast<AmqpTPCTransaction*> (&txn);
    if (atxn == 0)
        throw qpid::broker::InvalidTransactionContextException();
    try {
        atxn->sqlCommit();
    }
    catch(_com_error &e) {
        throw ADOException("Error preparing", e, atxn->dbConn()->getErrors());
    }
    atxn->setPrepared();
*/
}

void RethinkDBProvider::commit(qpid::broker::TransactionContext& /*txn*/)
{
    QPID_LOG(notice, "RethinkDBProvider::commit");

/*
    (void)initState();     // Ensure this thread is initialized

    // One-phase transactions simply commit the outer SQL transaction
    // that was begun on begin(). Two-phase transactions are different -
    // the SQL transaction started on begin() was committed on prepare()
    // so all the SQL records reflecting the enqueue/dequeue actions for
    // the transaction are recorded but with xid markups on them to reflect
    // that they are prepared but not committed. Now go back and remove
    // the markups, deleting those marked for removal.

    AmqpTPCTransaction *p2txn = dynamic_cast<AmqpTPCTransaction*> (&txn);
    if (p2txn == 0) {
        AmqpTransaction *p1txn = dynamic_cast<AmqpTransaction*> (&txn);
        if (p1txn == 0)
            throw qpid::broker::InvalidTransactionContextException();
        p1txn->sqlCommit();
        return;
    }

    DatabaseConnection *db(p2txn->dbConn());
    TplRecordset rsTpl;
    MessageMapRecordset rsMessageMap;
    try {
        db->beginTransaction();
        rsTpl.open(db, TblTpl);
        rsMessageMap.open(db, TblMessageMap);
        rsMessageMap.commitPrepared(p2txn->getXid());
        rsTpl.remove(p2txn->getXid());
        db->commitTransaction();
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        db->rollbackTransaction();
        throw ADOException("Error committing transaction", e, errs);
    }
*/
}

void RethinkDBProvider::abort(qpid::broker::TransactionContext& /*txn*/)
{
    QPID_LOG(notice, "RethinkDBProvider::abort");

/*
    (void)initState();     // Ensure this thread is initialized

    // One-phase and non-prepared two-phase transactions simply abort
    // the outer SQL transaction that was begun on begin(). However, prepared
    // two-phase transactions are different - the SQL transaction started
    // on begin() was committed on prepare() so all the SQL records
    // reflecting the enqueue/dequeue actions for the transaction are
    // recorded but with xid markups on them to reflect that they are
    // prepared but not committed. Now go back and remove the markups,
    // deleting those marked for addition.

    AmqpTPCTransaction *p2txn = dynamic_cast<AmqpTPCTransaction*> (&txn);
    if (p2txn == 0 || !p2txn->isPrepared()) {
        AmqpTransaction *p1txn = dynamic_cast<AmqpTransaction*> (&txn);
        if (p1txn == 0)
            throw qpid::broker::InvalidTransactionContextException();
        p1txn->sqlAbort();
        return;
    }

    DatabaseConnection *db(p2txn->dbConn());
    TplRecordset rsTpl;
    MessageMapRecordset rsMessageMap;
    try {
        db->beginTransaction();
        rsTpl.open(db, TblTpl);
        rsMessageMap.open(db, TblMessageMap);
        rsMessageMap.abortPrepared(p2txn->getXid());
        rsTpl.remove(p2txn->getXid());
        db->commitTransaction();
    }
    catch(_com_error &e) {
        std::string errs = db->getErrors();
        db->rollbackTransaction();
        throw ADOException("Error committing transaction", e, errs);
    }


    (void)initState();     // Ensure this thread is initialized
    AmqpTransaction *atxn = dynamic_cast<AmqpTransaction*> (&txn);
    if (atxn == 0)
        throw qpid::broker::InvalidTransactionContextException();
    atxn->sqlAbort();
*/
}

void RethinkDBProvider::collectPreparedXids(std::set<std::string>& /*xids*/)
{
    QPID_LOG(notice, "RethinkDBProvider::collectPreparedXids");

/*
    DatabaseConnection *db = initConnection();
    try {
        TplRecordset rsTpl;
        rsTpl.open(db, TblTpl);
        rsTpl.recover(xids);
    }
    catch(_com_error &e) {
        throw ADOException("Error reading TPL", e, db->getErrors());
    }
*/
}

// @TODO Much of this recovery code is way too similar... refactor to
// a recover template method on BlobRecordset.
void RethinkDBProvider::recoverConfigs(qpid::broker::RecoveryManager& /*recoverer*/)
{
    QPID_LOG(notice, "RethinkDBProvider::recoverConfigs");

/*
    DatabaseConnection *db = 0;
    try {
        db = initConnection();
        BlobRecordset rsConfigs;
        rsConfigs.open(db, TblConfig);
        _RecordsetPtr p = (_RecordsetPtr)rsConfigs;
        if (p->BOF && p->EndOfFile)
            return;   // Nothing to do
        p->MoveFirst();
        while (!p->EndOfFile) {
            uint64_t id = p->Fields->Item["persistenceId"]->Value;
            long blobSize = p->Fields->Item["fieldTableBlob"]->ActualSize;
            BlobAdapter blob(blobSize);
            blob = p->Fields->Item["fieldTableBlob"]->GetChunk(blobSize);
            // Recreate the Config instance and reset its ID.
            broker::RecoverableConfig::shared_ptr config =
                recoverer.recoverConfig(blob);
            config->setPersistenceId(id);
            p->MoveNext();
        }
    }
    catch(_com_error &e) {
        throw ADOException("Error recovering configs",
                           e,
                           db ? db->getErrors() : "");
    }
*/
}

void RethinkDBProvider::recoverExchanges(qpid::broker::RecoveryManager& recoverer,
                                         ExchangeMap& exchangeMap)
{
    QPID_LOG(notice, "RethinkDBProvider::recoverExchanges");

    try {
        uint64_t max_exchanges = 0;
        R::Connection* conn = initConnection();
        R::Cursor cursor = R::db(options.databaseName).table(TblExchange).run(*conn);
        while (cursor.has_next()) {
            R::Datum exchange_data = cursor.next();
            uint64_t primary_key =
                static_cast<uint64_t>(exchange_data.extract_field("id").extract_number());
            R::Binary blob_data = exchange_data.extract_field("blob").extract_binary();

            qpid::framing::Buffer blob(
                const_cast<char*>(blob_data.data.data()), (uint32_t)blob_data.data.size());
            broker::RecoverableExchange::shared_ptr exchange = recoverer.recoverExchange(blob);
            exchange->setPersistenceId(primary_key);
            exchangeMap[primary_key] = exchange;

            fprintf(stderr, "   recovered exchange(id=%lu, name=%s)\n",
                primary_key, exchange->getName().c_str());
            max_exchanges = std::max(max_exchanges, primary_key);
        }

        // start sequence generation one after highest id we recovered
        exchangeIdSequence.reset(max_exchanges + 1);
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::recoverExchanges exception: " + e.message);
        throw e;
    }
}

void RethinkDBProvider::recoverQueues(qpid::broker::RecoveryManager& recoverer,
                                      QueueMap& queueMap)
{
    QPID_LOG(notice, "RethinkDBProvider::recoverQueues");

    try {
        uint64_t max_queues = 0;
        R::Connection* conn = initConnection();
        R::Cursor cursor = R::db(options.databaseName).table(TblQueue).run(*conn);
        while (cursor.has_next()) {
            R::Datum queue_data = cursor.next();
            uint64_t primary_key =
                static_cast<uint64_t>(queue_data.extract_field("id").extract_number());
            R::Binary blob_data = queue_data.extract_field("blob").extract_binary();

            qpid::framing::Buffer blob(
                const_cast<char*>(blob_data.data.data()), (uint32_t)blob_data.data.size());
            broker::RecoverableQueue::shared_ptr queue = recoverer.recoverQueue(blob);
            queue->setPersistenceId(primary_key);
            queueMap[primary_key] = queue;

            fprintf(stderr, "   recovered queue(id=%lu, name=%s)\n",
                primary_key, queue->getName().c_str());
            max_queues = std::max(max_queues, primary_key);
        }

        // start sequence generation one after highest id we recovered
        queueIdSequence.reset(max_queues + 1);
    } catch (const R::Error& e) {
        QPID_LOG(error, "RethinkDBProvider::recoverQueues exception: " + e.message);
        throw e;
    }
}

void RethinkDBProvider::recoverBindings(qpid::broker::RecoveryManager& /*recoverer*/,
                                        const ExchangeMap& /*exchangeMap*/,
                                        const QueueMap& /*queueMap*/)
{
    QPID_LOG(notice, "RethinkDBProvider::recoverBindings");

    // R::Connection* conn = initConnection();
    // R::Cursor cursor = R::db(options.databaseName).table(TblBinding).run(*conn);
    // while (cursor.has_next()) {

    // }

/*
    if (rs->BOF && rs->EndOfFile)
        return;   // Nothing to do
    rs->MoveFirst();
    Binding b;
    IADORecordBinding *piAdoRecordBinding;
    rs->QueryInterface(__uuidof(IADORecordBinding),
                       (LPVOID *)&piAdoRecordBinding);
    piAdoRecordBinding->BindToRecordset(&b);
    while (!rs->EndOfFile) {
        long blobSize = rs->Fields->Item["fieldTableBlob"]->ActualSize;
        BlobAdapter blob(blobSize);
        blob = rs->Fields->Item["fieldTableBlob"]->GetChunk(blobSize);
        store::ExchangeMap::const_iterator exch = exchMap.find(b.exchangeId);
        if (exch == exchMap.end()) {
            std::ostringstream msg;
            msg << "Error recovering bindings; exchange ID " << b.exchangeId
                << " not found in exchange map";
            throw qpid::Exception(msg.str());
        }
        broker::RecoverableExchange::shared_ptr exchPtr = exch->second;
        store::QueueMap::const_iterator q = queueMap.find(b.queueId);
        if (q == queueMap.end()) {
            std::ostringstream msg;
            msg << "Error recovering bindings; queue ID " << b.queueId
                << " not found in queue map";
            throw qpid::Exception(msg.str());
        }
        broker::RecoverableQueue::shared_ptr qPtr = q->second;
        // The recovery manager wants the queue name, so get it from the
        // RecoverableQueue.
        std::string key(b.routingKey);
        exchPtr->bind(qPtr->getName(), key, blob);
        rs->MoveNext();
    }

    piAdoRecordBinding->Release();
*/

/*
    DatabaseConnection *db = 0;
    try {
        db = initConnection();
        BindingRecordset rsBindings;
        rsBindings.open(db, TblBinding);
        rsBindings.recover(recoverer, exchangeMap, queueMap);
    }
    catch(_com_error &e) {
        throw ADOException("Error recovering bindings",
                           e,
                           db ? db->getErrors() : "");
    }
*/
}

void RethinkDBProvider::recoverMessages(qpid::broker::RecoveryManager& /*recoverer*/,
                                        MessageMap& /*messageMap*/,
                                        MessageQueueMap& /*messageQueueMap*/)
{
    QPID_LOG(notice, "RethinkDBProvider::recoverMessages");

/*
    DatabaseConnection *db = 0;
    try {
        db = initConnection();
        MessageRecordset rsMessages;
        rsMessages.open(db, TblMessage);
        rsMessages.recover(recoverer, messageMap);

        MessageMapRecordset rsMessageMaps;
        rsMessageMaps.open(db, TblMessageMap);
        rsMessageMaps.recover(messageQueueMap);
    }
    catch(_com_error &e) {
        throw ADOException("Error recovering messages",
                           e,
                           db ? db->getErrors() : "");
    }
*/
}

void RethinkDBProvider::recoverTransactions(qpid::broker::RecoveryManager& /*recoverer*/,
                                            PreparedTransactionMap& /*dtxMap*/)
{
    QPID_LOG(notice, "RethinkDBProvider::recoverTransactions");

/*
    DatabaseConnection *db = initConnection();
    std::set<std::string> xids;
    try {
        TplRecordset rsTpl;
        rsTpl.open(db, TblTpl);
        rsTpl.recover(xids);
    }
    catch(_com_error &e) {
        throw ADOException("Error recovering TPL records", e, db->getErrors());
    }

    try {
        // Rebuild the needed RecoverableTransactions.
        for (std::set<std::string>::const_iterator iXid = xids.begin();
             iXid != xids.end();
             ++iXid) {
            boost::shared_ptr<DatabaseConnection> dbX(new DatabaseConnection);
            dbX->open(options.connectString, options.catalogName);
            std::auto_ptr<AmqpTPCTransaction> tx(new AmqpTPCTransaction(dbX,
                                                                        *iXid));
            tx->setPrepared();
            std::auto_ptr<qpid::broker::TPCTransactionContext> tc(tx);
            dtxMap[*iXid] = recoverer.recoverTransaction(*iXid, tc);
        }
    }
    catch(_com_error &e) {
        throw ADOException("Error recreating dtx connection", e);
    }
*/
}

////////////// Internal Methods

/*
State* RethinkDBProvider::initState()
{
    QPID_LOG(notice, "RethinkDBProvider::initState");

    State *state = dbState.get();   // See if thread has initialized
    if (!state) {
        state = new State;
        dbState.reset(state);
    }
    return state;
}
*/

/*
DatabaseConnection* RethinkDBProvider::initConnection(void)
{
    QPID_LOG(notice, "RethinkDBProvider::initConnection");

    State *state = initState();
    if (state->dbConn != 0)
        return state->dbConn;    // And the DatabaseConnection is set up too
    std::auto_ptr<DatabaseConnection> db(new DatabaseConnection);
    db->open(options.connectString, options.catalogName);
    state->dbConn = db.release();
    return state->dbConn;
}
*/

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

/*
    const std::string dbCmd = "CREATE DATABASE " + name;
    const std::string useCmd = "USE " + name;
    const std::string tableCmd = "CREATE TABLE ";
    const std::string colSpecs =
        " (persistenceId bigint PRIMARY KEY NOT NULL IDENTITY(1,1),"
        "  fieldTableBlob varbinary(MAX) NOT NULL)";
    const std::string bindingSpecs =
        " (exchangeId bigint REFERENCES tblExchange(persistenceId) NOT NULL,"
        "  queueId bigint REFERENCES tblQueue(persistenceId) NOT NULL,"
        "  routingKey varchar(255),"
        "  fieldTableBlob varbinary(MAX))";
    const std::string messageMapSpecs =
        " (messageId bigint REFERENCES tblMessage(persistenceId) NOT NULL,"
        "  queueId bigint REFERENCES tblQueue(persistenceId) NOT NULL,"
        "  prepareStatus tinyint CHECK (prepareStatus IS NULL OR "
        "    prepareStatus IN (1, 2)),"
        "  xid varbinary(512) REFERENCES tblTPL(xid)"
        "  CONSTRAINT CK_NoDups UNIQUE NONCLUSTERED (messageId, queueId) )";
    const std::string tplSpecs = " (xid varbinary(512) PRIMARY KEY NOT NULL)";
    // SET NOCOUNT ON added to prevent extra result sets from
    // interfering with SELECT statements. (Added by SQL Management)
    const std::string removeUnrefMsgsTrigger =
        "CREATE TRIGGER dbo.RemoveUnreferencedMessages "
        "ON  tblMessageMap AFTER DELETE AS BEGIN "
        "SET NOCOUNT ON; "
        "DELETE FROM tblMessage "
        "WHERE tblMessage.persistenceId IN "
        "  (SELECT messageId FROM deleted) AND"
        "  NOT EXISTS(SELECT * FROM tblMessageMap"
        "             WHERE tblMessageMap.messageId IN"
        "               (SELECT messageId FROM deleted)) "
        "END";

    _variant_t unused;
    _bstr_t dbStr = dbCmd.c_str();
    _ConnectionPtr conn(*db);
    try {
        conn->Execute(dbStr, &unused, adExecuteNoRecords);
        _bstr_t useStr = useCmd.c_str();
        conn->Execute(useStr, &unused, adExecuteNoRecords);
        std::string makeTable = tableCmd + TblQueue + colSpecs;
        _bstr_t makeTableStr = makeTable.c_str();
        conn->Execute(makeTableStr, &unused, adExecuteNoRecords);
        makeTable = tableCmd + TblExchange + colSpecs;
        makeTableStr = makeTable.c_str();
        conn->Execute(makeTableStr, &unused, adExecuteNoRecords);
        makeTable = tableCmd + TblConfig + colSpecs;
        makeTableStr = makeTable.c_str();
        conn->Execute(makeTableStr, &unused, adExecuteNoRecords);
        makeTable = tableCmd + TblMessage + colSpecs;
        makeTableStr = makeTable.c_str();
        conn->Execute(makeTableStr, &unused, adExecuteNoRecords);
        makeTable = tableCmd + TblBinding + bindingSpecs;
        makeTableStr = makeTable.c_str();
        conn->Execute(makeTableStr, &unused, adExecuteNoRecords);
        makeTable = tableCmd + TblTpl + tplSpecs;
        makeTableStr = makeTable.c_str();
        conn->Execute(makeTableStr, &unused, adExecuteNoRecords);
        makeTable = tableCmd + TblMessageMap + messageMapSpecs;
        makeTableStr = makeTable.c_str();
        conn->Execute(makeTableStr, &unused, adExecuteNoRecords);
        _bstr_t addTriggerStr = removeUnrefMsgsTrigger.c_str();
        conn->Execute(addTriggerStr, &unused, adExecuteNoRecords);
    }
    catch(_com_error &e) {
        throw ADOException("MSSQL can't create " + name, e, db->getErrors());
    }
*/
}

void RethinkDBProvider::dump()
{
  // dump all db records to qpid_log
  QPID_LOG(notice, "DB Dump: (not dumping anything)");
  //  rsQueues.dump();
}


}}} // namespace qpid::store::rethinkdb
