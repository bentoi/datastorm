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

    void forward(const Ice::ByteSeq&, const Ice::Current&) const;
    void destroy();

    const std::shared_ptr<DataStormContract::NodePrx> getPublicNode() const { return _publicNode; }
    const std::shared_ptr<Ice::Connection>& getConnection() const { return _connection; }

protected:

    const std::shared_ptr<Instance> _instance;
    const std::shared_ptr<TraceLevels> _traceLevels;
    const std::shared_ptr<DataStormContract::NodePrx> _node;
    const std::shared_ptr<Ice::Connection> _connection;

    std::shared_ptr<DataStormContract::NodePrx> _publicNode;
    std::shared_ptr<DataStormContract::LookupPrx> _lookup;
};

}
