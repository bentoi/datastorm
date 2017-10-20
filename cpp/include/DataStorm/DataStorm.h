// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <DataStorm/Config.h>

#include <Ice/Communicator.h>
#include <Ice/Initialize.h>
#include <Ice/InputStream.h>
#include <Ice/OutputStream.h>

#include <DataStorm/Sample.h>
#include <DataStorm/Node.h>
#include <DataStorm/InternalI.h>
#include <DataStorm/InternalT.h>

#include <regex>

/**
 * \mainpage %DataStorm API Reference
 *
 * \section namespaces Namespaces
 *
 * @ref DataStorm — The %DataStorm core library.
 */
namespace DataStorm
{

template<typename K, typename V, typename SFC> class KeyReader;
template<typename K, typename V, typename SF, typename SFC> class KeyWriter;
template<typename K, typename V, typename SFC> class FilteredReader;

/**
 * The Encoder template provides methods to encode and decode user types.
 *
 * The encoder template can be specialized to provide encodeling and un-encodeling
 * methods for types that don't support being encodeled with Ice. By default, the
 * Ice encodeling is used if no Encoder template specialization is provided for the
 * type.
 */
template<typename T>
struct Encoder
{
    /**
     * Marshals the given value. This method encodes the given value and returns the
     * resulting byte sequence. The factory parameter is provided to allow the implementation
     * to retrieve configuration or any other information required by the marhsalling.
     *
     * @see decode
     *
     * @param communicator The communicator associated with the node
     * @param value The value to encode
     * @return The resulting byte sequence
     */
    static std::vector<unsigned char> encode(const std::shared_ptr<Ice::Communicator>&, const T&);

    /**
     * Unencodes a value. This method decodes the given byte sequence and returns the
     * resulting value. The factory parameter is provided to allow the implementation
     * to retrieve configuration or any other information required by the un-encodeling.
     *
     * @see encode
     *
     * @param communicator The communicator associated with the node
     * @param value The byte sequence to decode
     * @return The resulting value
     */
    static T decode(const std::shared_ptr<Ice::Communicator>&, const std::vector<unsigned char>&);
};

/**
 * A sample provides information about an update of a data element.
 *
 * The Sample template provides access to key, value and type of
 * an update to a data element.
 *
 */
template<typename Key, typename Value>
class Sample
{
public:

    using KeyType = Key;
    using ValueType = Value;

    /**
     * The type of the sample.
     *
     * @return The sample type.
     */
    SampleType getType() const;

    /**
     * The key of the sample.
     *
     * @return The sample key.
     */
    Key getKey() const;

    /**
     * The value of the sample.
     *
     * Depending on the sample type, the sample value might not always be
     * available. It is for instance the case if the sample type is Remove.
     *
     * @return The sample value.
     */
    Value getValue() const;

    /**
     * The timestamp of the sample.
     *
     * The timestamp is generated by the writer. It corresponds to time at
     * which the sample was sent.
     *
     * TODO: use C++11 type.
     *
     * @return The timestamp.
     */
    IceUtil::Time getTimeStamp() const;

    /**
     * The origin of the sample.
     *
     * The origin of the sample identifies uniquely on the node the writer
     * that created the sample. It's a tupple composed of the node session
     * identity, the topic and writer opaque identifiers.
     *
     * @return The tuple that uniquely identifies on the node the origin
     *         of the sample.
     */
    std::tuple<std::string, long long int, long long int> getOrigin() const;

    /** @private */
    Sample(const std::shared_ptr<DataStormInternal::Sample>&);

private:

    std::shared_ptr<DataStormInternal::SampleT<Key, Value>> _impl;
};

/**
 * The RegexKeyFilter template filters keys matching a regular expression.
 **/
template<typename T>
class RegexFilter
{
public:

    /**
     * Construct the filter with the given filter criteria.
     *
     * @param criteria The filter criteria.
     */
    RegexFilter(const std::string& criteria) : _regex(criteria)
    {
    }

    /**
     * Returns wether or not the value matches the regular expression.
     *
     * @param value The value to match against the regular expression.
     * @return True if the value matches the regular expression, false otherwise.
     */
    bool match(const T& value) const
    {
        std::ostringstream os;
        os << value;
        return std::regex_match(os.str(), _regex);
    }

private:

    std::regex _regex;
};

/**
 * The SampleTypeFilter template filters samples based on a set of sample types.
 **/
template<typename Key, typename Value>
struct SampleTypeFilter
{
public:

    /**
     * Construct the filter with the given types criteria.
     *
     * @param criteria The filter criteria.
     */
    SampleTypeFilter(std::vector<SampleType> criteria) : _types(std::move(criteria))
    {
    }

    /**
     * Returns wether or not the sample matches the sample types.
     *
     * @param sample The sample to match against the filter.
     * @return True if the sample type matches the filter sample types, false otherwise.
     */
    bool match(const Sample<Key, Value>& sample) const
    {
        return std::find(_types.begin(), _types.end(), sample.getType()) != _types.end();
    }

private:

    const std::vector<SampleType> _types;
};

std::ostream&
operator<<(std::ostream& os, SampleType sampleType)
{
    switch(sampleType)
    {
    case SampleType::Add:
        os << "Add";
        break;
    case SampleType::Update:
        os << "Update";
        break;
    case SampleType::Remove:
        os << "Remove";
        break;
    default:
        os << static_cast<int>(sampleType);
        break;
    }
    return os;
}

std::ostream&
operator<<(std::ostream& os, const std::vector<SampleType>& types)
{
    os << "[";
    for(auto p = types.begin(); p != types.end(); ++p)
    {
        if(p != types.begin())
        {
            os << ',';
        }
        os << *p;
    }
    os << "]";
    return os;
}

template<typename K, typename V>
std::ostream&
operator<<(std::ostream& os, const Sample<K, V>& sample)
{
    os << sample.getValue();
    return os;
}

/**
 * The Topic class allows to construct Reader and Writer objects.
 */
template<typename Key, typename Value, typename KeyFilter=void, typename KeyFilterCriteria=void>
class Topic
{
public:

    using KeyType = Key;
    using ValueType = Value;
    using KeyFilterType = KeyFilter;
    using KeyFilterCriteriaType = KeyFilterCriteria;

    /**
     * Construct a new Topic for the topic with the given name.
     *
     * @param node The DataStorm node
     * @param name The name of the topic
     */
    Topic(Node&, const std::string&);

    /**
     * Construct a new Topic by taking ownership of the given topic.
     *
     * @param topic The topic to transfer ownership from.
     */
    Topic(Topic&&);

    /**
     * Destruct the Topic. This disconnects the topic from peers.
     */
    ~Topic();

    /**
     * Indicates whether or not topic writers are online.
     *
     * @return True if topic writers are connected, false otherwise.
     */
    bool hasWriters() const;

    /**
     * Wait for given number of topic writers to be online.
     *
     * @param count The number of topic writers to wait.
     */
    void waitForWriters(unsigned int = 1) const;

    /**
     * Wait for topic writers to be offline.
     */
    void waitForNoWriters() const;

    /**
     * Indicates whether or not topic readers are online.
     *
     * @return True if topic readers are connected, false otherwise.
     */
    bool hasReaders() const;

    /**
     * Wait for given number of topic readers to be online.
     *
     * @param count The number of topic readers to wait.
     */
    void waitForReaders(unsigned int = 1) const;

    /**
     * Wait for topic readers to be offline.
     */
    void waitForNoReaders() const;

private:

    std::shared_ptr<DataStormInternal::TopicReader> getReader() const;
    std::shared_ptr<DataStormInternal::TopicWriter> getWriter() const;
    std::shared_ptr<Ice::Communicator> getCommunicator() const;

    template<typename, typename, typename, typename> friend class KeyWriter;
    template<typename, typename, typename> friend class KeyReader;
    template<typename, typename, typename> friend class FilteredReader;

    const std::string _name;
    const std::shared_ptr<DataStormInternal::TopicFactory> _topicFactory;
    const std::shared_ptr<DataStormInternal::KeyFactoryT<Key>> _keyFactory;
    const std::shared_ptr<DataStormInternal::FilterFactoryT<KeyFilter,
                                                            KeyFilterCriteria,
                                                            DataStormInternal::KeyT<Key>>> _filterFactory;

    mutable std::mutex _mutex;
    mutable std::shared_ptr<DataStormInternal::TopicReader> _reader;
    mutable std::shared_ptr<DataStormInternal::TopicWriter> _writer;
};

/**
 * The Reader class is used to retrieve samples for a data element.
 */
template<typename Key, typename Value>
class Reader
{
public:

    using KeyType = Key;
    using ValueType = Value;

    /**
     * Destruct the reader. The destruction of the reader disconnects
     * the reader from the writers.
     */
    ~Reader();

    /**
     * Indicates whether or not writers are online.
     *
     * @return True if writers are connected, false otherwise.
     */
    bool hasWriters() const;

    /**
     * Wait for given number of writers to be online.
     *
     * @param count The number of writers to wait.
     */
    void waitForWriters(unsigned int = 1) const;

    /**
     * Wait for writers to be offline.
     */
    void waitForNoWriters() const;

    /**
     * Returns the number of times the data element was instantiated.
     *
     * This returns the number of times {@link Writer::add} was called for the
     * data element.
     */
    int getInstanceCount() const;

    /**
     * Returns all the data samples available with this reader.
     *
     * @return The data samples.
     */
    std::vector<Sample<Key, Value>> getAll() const;

    /**
     * Returns all the unread data samples.
     *
     * @return The unread data samples.
     */
    std::vector<Sample<Key, Value>> getAllUnread();

    /**
     * Wait for given number of unread data samples to be available.
     */
    void waitForUnread(unsigned int = 1) const;

    /**
     * Returns wether or not data unread data samples are available.
     */
    bool hasUnread() const;

    /**
     * Returns the next unread data sample.
     *
     * @return The unread data sample.
     */
    Sample<Key, Value> getNextUnread();

protected:

    /** @private */
    Reader(const std::shared_ptr<DataStormInternal::DataReader>& impl) : _impl(impl)
    {
    }

    /** @private */
    Reader(Reader&& reader) : _impl(std::move(reader._impl))
    {
    }

private:

    std::shared_ptr<DataStormInternal::DataReader> _impl;
};

/**
 * The key reader to read the data element associated with a given key.
 */
template<typename Key, typename Value, typename SampleFilterCriteria=void>
class KeyReader : public Reader<Key, Value>
{
public:

    /**
     * Construct a new reader for the given key. The construction of the reader
     * connects the reader to writers with a matching key.
     *
     * @param topic The topic.
     * @param key The key of the data element to read.
     */
    template<typename KeyFilter, typename KeyFilterCriteria>
    KeyReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>&, Key);

    /**
     * Construct a new reader for the given keys. The construction of the reader
     * connects the reader to writers with matching keys.
     *
     * @param topic The topic.
     * @param keys The keys of the data elements to read.
     */
    template<typename KeyFilter, typename KeyFilterCriteria>
    KeyReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>&, std::vector<Key>);

    /**
     * Construct a new reader for the given key and sample filter criteria. The
     * construction of the reader connects the reader to writers with a matching key.
     *
     * @param topic The topic.
     * @param key The key of the data element to read.
     * @param criteria The sample filter criteria used by writers to filter the samples
     */
    template<typename KeyFilter, typename KeyFilterCriteria>
    KeyReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>&, Key, SampleFilterCriteria);

    /**
     * Construct a new reader for the given keys and sample filter criteria. The
     * construction of the reader connects the reader to writers with matching keys.
     *
     * @param topic The topic reader.
     * @param keys The keys of the data elements to read.
     * @param criteria The sample filter criteria used by writers to filter the samples.
     */
    template<typename KeyFilter, typename KeyFilterCriteria>
    KeyReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>&, std::vector<Key>, SampleFilterCriteria);

    /**
     * Transfers the given reader to this reader.
     *
     * @param reader The reader.
     **/
    KeyReader(KeyReader&&);
};

template<typename K, typename V, typename KF, typename KFC, typename SFC=void>
KeyReader<K, V, SFC>
makeKeyReader(Topic<K, V, KF, KFC>& topic, typename Topic<K, V, KF, KFC>::KeyType key)
{
    return KeyReader<K, V, SFC>(topic, key);
}

template<typename K, typename V, typename KF, typename KFC, typename SFC>
KeyReader<K, V, SFC>
makeKeyReader(Topic<K, V, KF, KFC>& topic, typename Topic<K, V, KF, KFC>::KeyType key, SFC sampleFilterCriteria)
{
    return KeyReader<K, V, SFC>(topic, key, sampleFilterCriteria);
}

/**
 * Key reader template specialization for key readers with no sample filter.
 */
template<typename Key, typename Value>
class KeyReader<Key, Value, void> : public Reader<Key, Value>
{
public:

    /**
     * Construct a new reader for the given key. The construction of the reader
     * connects the reader to writers with a matching key.
     *
     * @param topic The topic.
     * @param key The key of the data element to read.
     */
    template<typename KeyFilter, typename KeyFilterCriteria>
    KeyReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>&, Key);

    /**
     * Construct a new reader for the given keys. The construction of the reader
     * connects the reader to writers with matching keys.
     *
     * @param topic The topic.
     * @param keys The keys of the data elements to read.
     */
    template<typename KeyFilter, typename KeyFilterCriteria>
    KeyReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>&, std::vector<Key>);

    /**
     * Transfers the given reader to this reader.
     *
     * @param reader The reader.
     **/
    KeyReader(KeyReader&&);
};

/**
 * The filtered reader to read data elements whose key match a given filter.
 */
template<typename Key, typename Value, typename SampleFilterCriteria=void>
class FilteredReader : public Reader<Key, Value>
{
public:

    /**
     * Construct a new reader for the given key filter. The construction of the reader
     * connects the reader to writers whose key matches the key filter criteria.
     *
     * @param topic The topic.
     * @param criteria The filter criteria.
     */
    template<typename KeyFilter, typename KeyFilterCriteria>
    FilteredReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>&,
                   typename Topic<Key, Value, KeyFilter, KeyFilterCriteria>::KeyFilterCriteriaType);

    /**
     * Construct a new reader for the given key filter and sample filter criterias. The
     * construction of the reader connects the reader to writers whose key matches the
     * key filter criteria.
     *
     * @param topic The topic.
     * @param criteria The key filter criteria used by writers to filter the key.
     * @param sampleCriteria The sample filter criteria used by writers to filter the samples
     */
    template<typename KeyFilter, typename KeyFilterCriteria>
    FilteredReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>&,
                   typename Topic<Key, Value, KeyFilter, KeyFilterCriteria>::KeyFilterCriteriaType,
                   SampleFilterCriteria);

    /**
     * Transfers the given reader to this reader.
     *
     * @param reader The reader.
     **/
    FilteredReader(FilteredReader&&);
};

template<typename K, typename V, typename KF, typename KFC, typename SFC=void>
FilteredReader<K, V, SFC>
makeFilteredReader(Topic<K, V, KF, KFC>& topic, typename Topic<K, V, KF, KFC>::KeyFilterCriteriaType filter)
{
    return FilteredReader<K, V, SFC>(topic, filter);
}

template<typename K, typename V, typename KF, typename KFC, typename SFC>
FilteredReader<K, V, SFC>
makeFilteredReader(Topic<K, V, KF, KFC>& topic,
                   typename Topic<K, V, KF, KFC>::KeyFilterCriteriaType filter,
                   SFC sampleFilterCriteria)
{
    return FilteredReader<K, V, SFC>(topic, filter, sampleFilterCriteria);
}

/**
 * Filtered reader template specialization for filtered readers with no sample filter.
 */

template<typename Key, typename Value>
class FilteredReader<Key, Value, void> : public Reader<Key, Value>
{
public:

    /**
     * Construct a new reader for the given key filter. The construction of the reader
     * connects the reader to writers whose key matches the key filter criteria.
     *
     * @param topic The topic.
     * @param criteria The filter criteria.
     */
    template<typename KeyFilter, typename KeyFilterCriteria>
    FilteredReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>&,
                   typename Topic<Key, Value, KeyFilter, KeyFilterCriteria>::KeyFilterCriteriaType);

    /**
     * Transfers the given reader to this reader.
     *
     * @param reader The reader.
     **/
    FilteredReader(FilteredReader&&);
};

/**
 * The Writer class is used to write samples for a data element.
 */
template<typename Key, typename Value>
class Writer
{
public:

    using KeyType = Key;
    using ValueType = Value;

    /**
     * Destruct the writer. The destruction of the writer disconnects
     * the writer from the readers.
     */
    ~Writer();

    /**
     * Indicates whether or not readers are online.
     *
     * @return True if readers are connected, false otherwise.
     */
    bool hasReaders() const;

    /**
     * Wait for given number of readers to be online.
     *
     * @param count The number of readers to wait.
     */
    void waitForReaders(unsigned int = 1) const;

    /**
     * Wait for readers to be offline.
     */
    void waitForNoReaders() const;

    /**
     * Add the data element. This generates an {@link Add} data sample with the
     * given value.
     *
     * @param value The data element value.
     */
    void add(const Value&);

    /**
     * Update the data element. This generates an {@link Update} data sample with the
     * given value.
     *
     * @param value The data element value.
     */
    void update(const Value&);

    /**
     * Remove the data element. This generates a {@link Remove} data sample.
     */
    void remove();

protected:

    /** @private */
    Writer(const std::shared_ptr<DataStormInternal::DataWriter>& impl) : _impl(impl)
    {
    }

    /** @private */
    Writer(Writer&& writer) : _impl(std::move(writer._impl))
    {
    }

private:

    std::shared_ptr<DataStormInternal::DataWriter> _impl;
};

/**
 * The key writer to write the data element associated with a given key.
 */
template<typename Key, typename Value, typename SampleFilter=void, typename SampleFilterCriteria=void>
class KeyWriter : public Writer<Key, Value>
{
public:

    /**
     * Construct a new writer for the given key. The construction of the writer
     * connects the writer to readers with a matching key.
     *
     * @param topic The topic.
     * @param key The key of the data element to write.
     */
    template<typename KeyFilter, typename KeyFilterCriteria>
    KeyWriter(Topic<Key, Value, KeyFilter, KeyFilterCriteria>&, Key);

    /**
     * Transfers the given writer to this writer.
     *
     * @param writer The writer.
     **/
    KeyWriter(KeyWriter&&);
};

template<typename K, typename V, typename KF, typename KFC>
KeyWriter<K, V, void, void>
makeKeyWriter(Topic<K, V, KF, KFC>& topic, typename Topic<K, V, KF, KFC>::KeyType key)
{
    return KeyWriter<K, V, void, void>(topic, key);
}

template<typename SF, typename SFC, typename K, typename V, typename KF, typename KFC>
KeyWriter<K, V, SF, SFC>
makeKeyWriter(Topic<K, V, KF, KFC>& topic, typename Topic<K, V, KF, KFC>::KeyType key)
{
    return KeyWriter<K, V, SF, SFC>(topic, key);
}

}


//
// Public template based API implementation
//

namespace DataStorm
{

//
// Encoder template implementation
//
template<typename T> std::vector<unsigned char>
Encoder<T>::encode(const std::shared_ptr<Ice::Communicator>& communicator, const T& value)
{
    std::vector<unsigned char> v;
    Ice::OutputStream stream(communicator);
    stream.write(value);
    stream.finished(v);
    return v;
}

template<typename T> T
Encoder<T>::decode(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& value)
{
    T v;
    if(value.empty())
    {
        v = T();
    }
    else
    {
        Ice::InputStream(communicator, value).read(v);
    }
    return v;
}

//
// Sample template implementation
//
template<typename Key, typename Value> SampleType
Sample<Key, Value>::getType() const
{
    return _impl->type;
}

template<typename Key, typename Value> Key
Sample<Key, Value>::getKey() const
{
    return _impl->getKey();
}

template<typename Key, typename Value> Value
Sample<Key, Value>::getValue() const
{
    return _impl->getValue();
}

template<typename Key, typename Value> IceUtil::Time
Sample<Key, Value>::getTimeStamp() const
{
    return IceUtil::Time::milliSeconds(_impl->timestamp);
}

template<typename Key, typename Value> std::tuple<std::string, long long int, long long int>
Sample<Key, Value>::getOrigin() const
{
    return std::make_tuple<std::string, long long int, long long int>(_impl->session, _impl->topic, _impl->element);
}

template<typename Key, typename Value>
Sample<Key, Value>::Sample(const std::shared_ptr<DataStormInternal::Sample>& impl) :
    _impl(std::static_pointer_cast<DataStormInternal::SampleT<Key, Value>>(impl))
{
}

//
// Reader template implementation
//
template<typename Key, typename Value>
Reader<Key, Value>::~Reader()
{
    if(_impl)
    {
        _impl->destroy();
    }
}

template<typename Key, typename Value> bool
Reader<Key, Value>::hasWriters() const
{
    return _impl->hasWriters();
}

template<typename Key, typename Value> void
Reader<Key, Value>::waitForWriters(unsigned int count) const
{
    _impl->waitForWriters(count);
}

template<typename Key, typename Value> void
Reader<Key, Value>::waitForNoWriters() const
{
    _impl->waitForWriters(-1);
}

template<typename Key, typename Value> int
Reader<Key, Value>::getInstanceCount() const
{
    return _impl->getInstanceCount();
}

template<typename Key, typename Value> std::vector<Sample<Key, Value>>
Reader<Key, Value>::getAll() const
{
    auto all = _impl->getAll();
    std::vector<Sample<Key, Value>> samples;
    samples.reserve(all.size());
    for(const auto& sample : all)
    {
        samples.emplace_back(sample);
    }
    return samples;
}

template<typename Key, typename Value> std::vector<Sample<Key, Value>>
Reader<Key, Value>::getAllUnread()
{
    auto unread = _impl->getAllUnread();
    std::vector<Sample<Key, Value>> samples;
    samples.reserve(unread.size());
    for(auto sample : unread)
    {
        samples.emplace_back(sample);
    }
    return samples;
}

template<typename Key, typename Value> void
Reader<Key, Value>::waitForUnread(unsigned int count) const
{
    _impl->waitForUnread(count);
}

template<typename Key, typename Value> bool
Reader<Key, Value>::hasUnread() const
{
    return _impl->hasUnread();
}

template<typename Key, typename Value> Sample<Key, Value>
Reader<Key, Value>::getNextUnread()
{
    return Sample<Key, Value>(_impl->getNextUnread());
}

template<typename Key, typename Value, typename SampleFilterCriteria>
template<typename KeyFilter, typename KeyFilterCriteria>
KeyReader<Key, Value, SampleFilterCriteria>::KeyReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>& topic,
                                                       Key key,
                                                       SampleFilterCriteria criteria) :
    Reader<Key, Value>(topic.getReader()->create({ topic._keyFactory->create(std::move(key)) },
                                                 Encoder<SampleFilterCriteria>::encode(topic.getCommunicator(),
                                                                                       criteria)))
{
}

template<typename Key, typename Value, typename SampleFilterCriteria>
template<typename KeyFilter, typename KeyFilterCriteria>
KeyReader<Key, Value, SampleFilterCriteria>::KeyReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>& topic,
                                                       Key key) :
    Reader<Key, Value>(topic.getReader()->create({ topic._keyFactory->create(std::move(key)) }))
{
}

template<typename Key, typename Value, typename SampleFilterCriteria>
template<typename KeyFilter, typename KeyFilterCriteria>
KeyReader<Key, Value, SampleFilterCriteria>::KeyReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>& topic,
                                                       std::vector<Key> keys,
                                                       SampleFilterCriteria criteria) :
    Reader<Key, Value>(topic.getReader()->create(topic._keyFactory->create(std::move(keys)),
                                                 Encoder<SampleFilterCriteria>::encode(topic.getCommunicator(),
                                                                                       criteria)))
{
}

template<typename Key, typename Value, typename SampleFilterCriteria>
template<typename KeyFilter, typename KeyFilterCriteria>
KeyReader<Key, Value, SampleFilterCriteria>::KeyReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>& topic,
                                                       std::vector<Key> keys) :
    Reader<Key, Value>(topic.getReader()->create(topic._keyFactory->create(std::move(keys))))
{
}

template<typename Key, typename Value, typename SampleFilterCriteria>
KeyReader<Key, Value, SampleFilterCriteria>::KeyReader(KeyReader<Key, Value, SampleFilterCriteria>&& reader) :
    Reader<Key, Value>(std::move(reader))
{
}

template<typename Key, typename Value>
template<typename KeyFilter, typename KeyFilterCriteria>
KeyReader<Key, Value, void>::KeyReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>& topic, Key key) :
    Reader<Key, Value>(topic.getReader()->create({ topic._keyFactory->create(std::move(key)) }))
{
}

template<typename Key, typename Value>
template<typename KeyFilter, typename KeyFilterCriteria>
KeyReader<Key, Value, void>::KeyReader(Topic<Key, Value, KeyFilter, KeyFilterCriteria>& topic, std::vector<Key> keys) :
    Reader<Key, Value>(topic.getReader()->create(topic._keyFactory->create(std::move(keys))))
{
}

template<typename Key, typename Value>
KeyReader<Key, Value, void>::KeyReader(KeyReader<Key, Value, void>&& reader) :
    Reader<Key, Value>(std::move(reader))
{
}

template<typename Key, typename Value, typename SampleFilterCriteria>
template<typename KeyFilter, typename KeyFilterCriteria>
FilteredReader<Key, Value, SampleFilterCriteria>::FilteredReader(
    Topic<Key, Value, KeyFilter, KeyFilterCriteria>& topic,
    typename Topic<Key, Value, KeyFilter, KeyFilterCriteria>::KeyFilterCriteriaType criteria,
    SampleFilterCriteria sampleCriteria) :
    Reader<Key, Value>(topic.getReader()->createFiltered(topic._filterFactory->create(std::move(criteria)),
                                                         Encoder<SampleFilterCriteria>::encode(topic.getCommunicator(),
                                                                                               sampleCriteria)))
{
}

template<typename Key, typename Value, typename SampleFilterCriteria>
template<typename KeyFilter, typename KeyFilterCriteria>
FilteredReader<Key, Value, SampleFilterCriteria>::FilteredReader(
    Topic<Key, Value, KeyFilter, KeyFilterCriteria>& topic,
    typename Topic<Key, Value, KeyFilter, KeyFilterCriteria>::KeyFilterCriteriaType criteria) :
    Reader<Key, Value>(topic.getReader()->createFiltered(topic._filterFactory->create(std::move(criteria))))
{
}

template<typename Key, typename Value, typename SampleFilterCriteria>
FilteredReader<Key, Value, SampleFilterCriteria>::FilteredReader(FilteredReader<Key, Value, SampleFilterCriteria>&& reader) :
    Reader<Key, Value>(std::move(reader))
{
}

template<typename Key, typename Value>
template<typename KeyFilter, typename KeyFilterCriteria>
FilteredReader<Key, Value, void>::FilteredReader(
    Topic<Key, Value, KeyFilter, KeyFilterCriteria>& topic,
    typename Topic<Key, Value, KeyFilter, KeyFilterCriteria>::KeyFilterCriteriaType criteria) :
    Reader<Key, Value>(topic.getReader()->createFiltered(topic._filterFactory->create(std::move(criteria))))
{
}

template<typename Key, typename Value>
FilteredReader<Key, Value, void>::FilteredReader(FilteredReader<Key, Value, void>&& reader) :
    Reader<Key, Value>(std::move(reader))
{
}

//
// Writer template implementation
//
template<typename Key, typename Value>
Writer<Key, Value>::~Writer()
{
    if(_impl)
    {
        _impl->destroy();
    }
}

template<typename Key, typename Value> bool
Writer<Key, Value>::hasReaders() const
{
    return _impl->hasReaders();
}

template<typename Key, typename Value> void
Writer<Key, Value>::waitForReaders(unsigned int count) const
{
    return _impl->waitForReaders(count);
}

template<typename Key, typename Value> void
Writer<Key, Value>::waitForNoReaders() const
{
    return _impl->waitForReaders(-1);
}

template<typename Key, typename Value> void
Writer<Key, Value>::add(const Value& value)
{
    _impl->publish(std::make_shared<DataStormInternal::SampleT<Key, Value>>(SampleType::Add, value));
}

template<typename Key, typename Value> void
Writer<Key, Value>::update(const Value& value)
{
    _impl->publish(std::make_shared<DataStormInternal::SampleT<Key, Value>>(SampleType::Update, value));
}

template<typename Key, typename Value> void
Writer<Key, Value>::remove()
{
    _impl->publish(std::make_shared<DataStormInternal::SampleT<Key, Value>>(SampleType::Remove, Value()));
}

template<typename Key, typename Value, typename SampleFilter, typename SampleFilterCriteria>
template<typename KeyFilter, typename KeyFilterCriteria>
KeyWriter<Key, Value, SampleFilter, SampleFilterCriteria>::KeyWriter(
    Topic<Key, Value, KeyFilter, KeyFilterCriteria>& topic,
    Key key) :
    Writer<Key, Value>(topic.getWriter()->create(topic._keyFactory->create(std::move(key)),
                                                 DataStormInternal::FilterFactoryT<
                                                     SampleFilter,
                                                     SampleFilterCriteria,
                                                     DataStormInternal::SampleT<Key, Value>>::createFactory()))
{
}

template<typename Key, typename Value, typename SampleFilter, typename SampleFilterCriteria>
KeyWriter<Key, Value, SampleFilter, SampleFilterCriteria>::KeyWriter(
    KeyWriter<Key, Value, SampleFilter, SampleFilterCriteria>&& writer) :
    Writer<Key, Value>(std::move(writer))
{
}

//
// Topic template implementation
//
template<typename Key, typename Value, typename KeyFilter, typename KeyFilterCriteria>
Topic<Key, Value, KeyFilter, KeyFilterCriteria>::Topic(Node& node, const std::string& name) :
    _name(name),
    _topicFactory(node._factory),
    _keyFactory(DataStormInternal::KeyFactoryT<Key>::createFactory()),
    _filterFactory(DataStormInternal::FilterFactoryT<KeyFilter,
                                                     KeyFilterCriteria,
                                                     DataStormInternal::KeyT<Key>>::createFactory())
{
}

template<typename Key, typename Value, typename KeyFilter, typename KeyFilterCriteria>
Topic<Key, Value, KeyFilter, KeyFilterCriteria>::Topic(Topic<Key, Value, KeyFilter, KeyFilterCriteria>&& topic) :
    _topicFactory(topic._topicFactory),
    _reader(std::move(topic._reader)),
    _writer(std::move(topic._writer)),
    _keyFactory(std::move(topic._keyFactory)),
    _filterFactory(std::move(topic._filterFactory))
{
}

template<typename Key, typename Value, typename KeyFilter, typename KeyFilterCriteria>
Topic<Key, Value, KeyFilter, KeyFilterCriteria>::~Topic()
{
    std::lock_guard<std::mutex> lock(_mutex);
    if(_reader)
    {
        _reader->destroy();
    }
    if(_writer)
    {
        _writer->destroy();
    }
}

template<typename Key, typename Value, typename KeyFilter, typename KeyFilterCriteria> bool
Topic<Key, Value, KeyFilter, KeyFilterCriteria>::hasWriters() const
{
    return getReader()->hasWriters();
}

template<typename Key, typename Value, typename KeyFilter, typename KeyFilterCriteria> void
Topic<Key, Value, KeyFilter, KeyFilterCriteria>::waitForWriters(unsigned int count) const
{
    getReader()->waitForWriters(count);
}

template<typename Key, typename Value, typename KeyFilter, typename KeyFilterCriteria> void
Topic<Key, Value, KeyFilter, KeyFilterCriteria>::waitForNoWriters() const
{
    getReader()->waitForWriters(-1);
}

template<typename Key, typename Value, typename KeyFilter, typename KeyFilterCriteria> bool
Topic<Key, Value, KeyFilter, KeyFilterCriteria>::hasReaders() const
{
    return getWriter()->hasReaders();
}

template<typename Key, typename Value, typename KeyFilter, typename KeyFilterCriteria> void
Topic<Key, Value, KeyFilter, KeyFilterCriteria>::waitForReaders(unsigned int count) const
{
    getWriter()->waitForReaders(count);
}

template<typename Key, typename Value, typename KeyFilter, typename KeyFilterCriteria> void
Topic<Key, Value, KeyFilter, KeyFilterCriteria>::waitForNoReaders() const
{
    getWriter()->waitForReaders(-1);
}

template<typename Key, typename Value, typename KeyFilter, typename KeyFilterCriteria>
std::shared_ptr<DataStormInternal::TopicReader>
Topic<Key, Value, KeyFilter, KeyFilterCriteria>::getReader() const
{
    std::lock_guard<std::mutex> lock(_mutex);
    if(!_reader)
    {
        _reader = _topicFactory->createTopicReader(_name,
                                                   _keyFactory,
                                                   _filterFactory,
                                                   std::make_shared<DataStormInternal::SampleFactoryT<Key, Value>>());
    }
    return _reader;
}

template<typename Key, typename Value, typename KeyFilter, typename KeyFilterCriteria>
std::shared_ptr<DataStormInternal::TopicWriter>
Topic<Key, Value, KeyFilter, KeyFilterCriteria>::getWriter() const
{
    std::lock_guard<std::mutex> lock(_mutex);
    if(!_writer)
    {
        _writer = _topicFactory->createTopicWriter(_name,
                                                   _keyFactory,
                                                   _filterFactory,
                                                   std::make_shared<DataStormInternal::SampleFactoryT<Key, Value>>());
    }
    return _writer;
}

template<typename Key, typename Value, typename KeyFilter, typename KeyFilterCriteria>
std::shared_ptr<Ice::Communicator>
Topic<Key, Value, KeyFilter, KeyFilterCriteria>::getCommunicator() const
{
    return _topicFactory->getCommunicator();
}

}