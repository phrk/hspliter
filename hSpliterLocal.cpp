#include <set>

#include "hSpliterLocal.h"


hSpliterLocal::KeyState::KeyState(bool _owned, bool _handled):
			owned(_owned),
			handled(_handled)
{
}

hSpliterLocal::KeyState::KeyState(std::string _owned, std::string _handled)
{
	owned = 0;
	handled = 0;
	if (_owned=="1")
	{
		owned = 1;
	}
	else if (_owned=="0")
	{
	}
	else
	{
		throw "KeyState::KeyState _owned undefined value: " + _owned;
	}
	
	if (_handled=="1")
	{
		handled = 1;
	}
	else if (_handled=="0")
	{
		handled = 0;
	}
	else
	{
		throw "KeyState::KeyState handled undefined value: " + _owned;
	}
}
		
		
hSpliterLocal::hSpliterLocal(ThriftClientPtr _client,
				std::string _ns,
				std::string _job,
				hSpliterClient::Mode mode,
				size_t key_step):
	m_client(_client),
	m_job(_job),
	m_key_step(key_step)
{
	m_ns = _client->namespace_open(_ns);
	if (mode == hSpliterClient::START)
	{
		Hypertable::ThriftGen::HqlResult result;
		_client->hql_query(result, m_ns, "drop table if exists "+_job);
		_client->hql_query(result, m_ns, "create table "+_job+\
				" (owned MAX_VERSIONS=1, handled MAX_VERSIONS=1)");
	}

	m_querier.reset(new htQuerier(_client, _ns, _job));
	m_writer.reset(new htCollWriterConc(_client, _ns, _job));
	
	m_input_scanner.reset(new htKeyScanner(_client, _ns, _job));
	std::set<std::string> state_colls;
	state_colls.insert("owned");
	state_colls.insert("handled");
	m_states_scanner.reset(new htCustomScanner(_client, _ns, _job, state_colls));
	
	loadStates();
	makeRanges();
}

void hSpliterLocal::loadStates()
{
	while (!m_states_scanner->end())
	{
		htLine line = m_states_scanner->getNextLine();
		m_key_states.insert(std::pair<std::string, KeyState> \
						(line.key,
								KeyState(line.cells["owned"],
										line.cells["handled"])));
	}
}

bool hSpliterLocal::isOwned(std::string key)
{
	std::tr1::unordered_map<std::string, KeyState>::iterator it = \
		m_key_states.find(key);
	
	if (it != m_key_states.end())
	{
		return it->second.owned;
	}
	else
	{
		throw "hSpliterLocal::isOwned no such key in states: " + key;
	}	
}

bool hSpliterLocal::isHandled(std::string key)
{
	std::tr1::unordered_map<std::string, KeyState>::iterator it = \
		m_key_states.find(key);
	
	if (it != m_key_states.end())
	{
		return it->second.handled;
	}
	else
	{
		throw "hSpliterLocal::isHandled no such key in states: " + key;
	}
}

void hSpliterLocal::makeRanges()
{
	bool beg_set = 0;//, end_set = 0;
	std::string beg_key, end_key;
	size_t nkeys;
	
	while (!m_input_scanner->end())
	{
		std::string key = m_input_scanner->getNextKey();
		if (!isOwned(key))
		{
			if (!beg_set)
			{
				beg_key = key;
				beg_set = 1;
				nkeys++;
			}
			else
			{
				end_key = key;
				nkeys++;
				if (nkeys >= m_key_step)
				{
					m_free_ranges.push(KeyRange(beg_key, end_key));
					beg_set = 0;
					nkeys = 0;
				}
			}
		}
		else
		{
			if (beg_set)
			{
				m_free_ranges.push(KeyRange(beg_key, end_key));
				beg_set = 0;
			}
		}
		
	}
}

hSpliterLocal::~hSpliterLocal()
{
}

KeyRange hSpliterLocal::getSplit()
{
	
}

bool hSpliterLocal::tryOwnKey(std::string key)
{
	
}

void hSpliterLocal::setKeyHandled(std::string key)
{
	
}
