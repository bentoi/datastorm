// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <DataStorm/Config.h>
#include <DataStorm/Contract.h>

#include <Ice/Ice.h>

namespace DataStormI
{

class LookupSessionI;
class CallbackExecutor;
class Instance;
class TraceLevels;

class LookupSessionManager : public std::enable_shared_from_this<LookupSessionManager>
{
public:

    LookupSessionManager(std::shared_ptr<Instance>);

    void init();

    void create(const std::shared_ptr<DataStormContract::NodePrx>&, const std::shared_ptr<Ice::Connection>&);

    void announceTopicReader(const std::string&,
                             const std::shared_ptr<DataStormContract::NodePrx>&,
                             const std::shared_ptr<Ice::Connection>& = nullptr) const;

    void announceTopicWriter(const std::string&,
                             const std::shared_ptr<DataStormContract::NodePrx>&,
                             const std::shared_ptr<Ice::Connection>& = nullptr) const;

    void announceTopics(const DataStormContract::StringSeq&,
                        const DataStormContract::StringSeq&,
                        const std::shared_ptr<DataStormContract::NodePrx>&,
                        const std::shared_ptr<Ice::Connection>& = nullptr) const;

    std::shared_ptr<LookupSessionI> getSession(const std::shared_ptr<DataStormContract::NodePrx>&) const;
    std::shared_ptr<LookupSessionI> getSession(const std::string&) const;

    const std::shared_ptr<Ice::Object>& getSessionForwarder() const
    {
        return _sessionForwarder;
    }

    void forward(const Ice::ByteSeq&, const Ice::Current&) const;

    void destroy() const;

private:

    void connect(const std::shared_ptr<DataStormContract::LookupPrx>&);

    void connected(const std::shared_ptr<DataStormContract::NodePrx>&,
                   const std::shared_ptr<DataStormContract::LookupPrx>&);

    void disconnected(const std::shared_ptr<DataStormContract::NodePrx>&,
                      const std::shared_ptr<DataStormContract::LookupPrx>&);

    void destroySession(const std::shared_ptr<DataStormContract::NodePrx>&);

    const std::shared_ptr<Instance> _instance;
    const std::shared_ptr<TraceLevels> _traceLevels;

    mutable std::mutex _mutex;

    std::map<Ice::Identity, std::shared_ptr<LookupSessionI>> _sessions;
    std::map<Ice::Identity, std::pair<std::shared_ptr<DataStormContract::NodePrx>,
                                      std::shared_ptr<DataStormContract::LookupPrx>>> _connectedTo;

    mutable std::shared_ptr<Ice::Connection> _exclude;
    std::shared_ptr<DataStormContract::LookupPrx> _forwarder;
    std::shared_ptr<Ice::Object> _sessionForwarder;
};

}
