// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <DataStorm/Config.h>

#include <Ice/Ice.h>

namespace DataStormI
{

class Instance;

class ForwarderManager : public Ice::BlobjectAsync
{
public:

    using Response = std::function<void(bool, const std::vector<Ice::Byte>&)>;
    using Exception = std::function<void(std::exception_ptr)>;

    ForwarderManager(const std::shared_ptr<Ice::ObjectAdapter>&, const std::string&);

    std::shared_ptr<Ice::ObjectPrx> add(std::function<void(Ice::ByteSeq, Response, Exception, const Ice::Current&)>);
    std::shared_ptr<Ice::ObjectPrx> add(std::function<void(Ice::ByteSeq, const Ice::Current&)>);
    std::shared_ptr<Ice::ObjectPrx> add(std::shared_ptr<Ice::ObjectPrx>);
    void remove(const Ice::Identity&);

private:

    virtual void ice_invokeAsync(Ice::ByteSeq,
                                 std::function<void(bool, const std::vector<Ice::Byte>&)>,
                                 std::function<void(std::exception_ptr)>,
                                 const Ice::Current&);

    const std::shared_ptr<Ice::ObjectAdapter> _adapter;
    const std::string _category;

    std::mutex _mutex;
    std::map<std::string, std::function<void(Ice::ByteSeq, Response, Exception, const Ice::Current&)>> _forwarders;
    unsigned int _nextId;
};

}
