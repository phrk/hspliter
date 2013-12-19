/* 
 * File:   hSpliterLocal.h
 * Author: phrk
 *
 * Created on December 12, 2013, 11:11 PM
 */

#ifndef HSPLITERLOCAL_H
#define	HSPLITERLOCAL_H

#include <tr1/unordered_map>

#include "hSpliterClient.h"
#include "../htdba/htCollScanner.h"
#include "../htdba/htCollWriter.h"
#include "../htdba/htQuerier.h"
#include "../htdba/htKeyScanner.h"
#include "../htdba/htCustomScanner.h"

class hSpliterLocal : public hSpliterClient
{
	ThriftClientPtr m_client;
	Hypertable::ThriftGen::Namespace m_ns;
	
	std::string m_job;
	size_t m_key_step;
	
	htQuerierPtr m_querier; 
	htKeyScannerPtr m_input_scanner;
	htCollScannerPtr m_states_scanner;
	htCollWriterConcPtr m_writer;
	
	std::tr1::unordered_map<std::string, bool> m_keys_handled;
	// key / timestamp
	std::tr1::unordered_map<std::string, uint64_t> m_keys_commiting;
	
	std::queue<KeyRange> m_free_ranges;
	
	uint64_t m_nkeys;
	uint64_t m_nreassigned;
	boost::atomic<uint64_t> m_nhandled;
	
	hAutoLockPtr m_lock;
	//std::queue<KeyRange> m_nothandled_ranges;
	
	//bool isOwned(std::string key);
	bool isCommiting(std::string key);
	bool isHandled(std::string key);
	
	void createDbAccessors(std::string _ns,
								std::string _job,
								std::string _input_table);
	void loadStates();
	void makeRanges();
	
public:
	
	hSpliterLocal(ThriftClientPtr _client,
				std::string _ns,
				std::string _input_table,
				std::string _job,
				hSpliterClient::Mode mode,
				size_t key_step = 10);
	
	virtual ~hSpliterLocal();
	virtual KeyRange getSplit();
	virtual bool tryKeyCommit(std::string key);
	virtual void setKeyCommited(std::string key);
};

/*
 * 
 * ns::job
 *	id	|	handled	
 * 
 */

#endif	/* HSPLITERLOCAL_H */

