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
	htCustomScannerPtr m_states_scanner;
	htCollWriterConcPtr m_writer;
	
	class KeyState
	{
		public:
		bool owned;
		bool handled;
		
		KeyState(bool _owned, bool _handled);
		KeyState(std::string _owned, std::string _handled);
	};
	
	std::tr1::unordered_map<std::string, KeyState> m_key_states;
	std::queue<KeyRange> m_free_ranges;
	std::queue<KeyRange> m_owned_ranges;
	
	bool isOwned(std::string key);
	bool isHandled(std::string key);
	
	void loadStates();
	void makeRanges();
	
public:
	
	hSpliterLocal(ThriftClientPtr _client,
				std::string _ns,
				std::string _job,
				hSpliterClient::Mode mode,
				size_t key_step = 10);
	
	virtual ~hSpliterLocal();
	virtual KeyRange getSplit();
	virtual bool tryOwnKey(std::string key);
	virtual void setKeyHandled(std::string key);
};

/*
 * 
 * ns::job
 *	id	|	owned	|	handled	
 * 
 */

#endif	/* HSPLITERLOCAL_H */

