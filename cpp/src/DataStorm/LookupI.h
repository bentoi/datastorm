// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <DataStorm/Contract.h>

namespace DataStormI
{

class Instance;

class LookupI : public DataStormContract::Lookup
{
public:

    LookupI(const std::shared_ptr<Instance>&);

    virtual void announceTopicReader(std::string, std::shared_ptr<DataStormContract::NodePrx>, const Ice::Current&);
    virtual void announceTopicWriter(std::string, std::shared_ptr<DataStormContract::NodePrx>, const Ice::Current&);

    virtual void announceTopics(DataStormContract::StringSeq,
                                DataStormContract::StringSeq,
                                std::shared_ptr<DataStormContract::NodePrx>,
                                const Ice::Current&);

    virtual std::shared_ptr<DataStormContract::NodePrx> createSession(std::shared_ptr<DataStormContract::NodePrx>,
                                                                      const Ice::Current&);

private:

    std::shared_ptr<Instance> _instance;
};

}
