// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <DataStorm/Contract.h>

#include <Ice/Ice.h>

namespace DataStormI
{

class Instance;
class TraceLevels;

class LookupSessionI : std::enable_shared_from_this<LookupSessionI>
{
public:

    LookupSessionI(std::shared_ptr<Instance>,
                   std::shared_ptr<DataStormContract::NodePrx>,
                   std::shared_ptr<Ice::Connection>);

    void destroy();

    const std::shared_ptr<DataStormContract::NodePrx>& getPublicNode() const { return _publicNode; }
    const std::shared_ptr<DataStormContract::LookupPrx>& getLookup() const { return _lookup; }
    const std::shared_ptr<Ice::Connection>& getConnection() const { return _connection; }
    template<typename T> std::shared_ptr<T> getSessionForwarder(const std::shared_ptr<T>& session) const
    {
        return Ice::uncheckedCast<T>(forwarder(session));
    }

private:

    std::shared_ptr<DataStormContract::SessionPrx>
    forwarder(const std::shared_ptr<DataStormContract::SessionPrx>&) const;

    const std::shared_ptr<Instance> _instance;
    const std::shared_ptr<TraceLevels> _traceLevels;
    const std::shared_ptr<DataStormContract::NodePrx> _node;
    const std::shared_ptr<Ice::Connection> _connection;

    std::shared_ptr<DataStormContract::NodePrx> _publicNode;
    std::shared_ptr<DataStormContract::LookupPrx> _lookup;
};

}
