#include "hspliter_tests.h"

void hSpliterTests::testLocal()
{
	htConnPoolPtr conn_pool(new htConnPool("localhost", 38080, 10));
	
	ThriftClientPtr client( new ThriftClient("localhost", 38080));
	const std::string ns_name = "test";
	Hypertable::ThriftGen::Namespace ns = client->namespace_open(ns_name);
	
	Hypertable::ThriftGen::HqlResult result;
	client->hql_query(result, ns, "drop table if exists split_input");
	client->hql_query(result, ns, "create table split_input (num MAX_VERSIONS=1)");
	client->close_namespace(ns);
	
	htCollWriterConc writer(conn_pool, ns_name, "split_input");
	
	uint32_t control_summ = 0;
	for (int i = 0; i<1000; i++) {
		char bf[255];
		sprintf(bf, "%d", i);
		writer.insertSync(KeyValue(bf, bf), "num");
		
		control_summ += i;
	}
	
	sleep(1);
	
	hSpliterLocalPtr spliter(new hSpliterLocal(conn_pool,
					 ns_name,
					"split_input",
					"split_job",
					hSpliterClient::START,
					20));
	KeyRange range = spliter->getSplit();
	htKeyScanner key_scanner (conn_pool, ns_name, "split_input", range);
	
	int i = 0;
	
	while (range.ok()) {
		std::cout << range.toString() << std::endl;
		key_scanner.reset(range);		

		while (!key_scanner.end() && i<=2) {
			std::string key = key_scanner.getNextKey();
			std::cout << key << " ";
			
			spliter->tryKeyCommit(key);
			spliter->setKeyCommited(key);
		}
		
		while (!key_scanner.end() && i>2) {
			std::string key = key_scanner.getNextKey();
			std::cout << key << " ";
			
			spliter->tryKeyCommit(key);
			spliter->setKeyCommited(key);
		}
		
		std::cout << std::endl;
		range = spliter->getSplit();
		i++;
		
		//if (i==10) break;
	}
	
	//spliter.reset();
}
