/* 
 * File:   hSpliterClient.h
 * Author: phrk
 *
 * Created on December 12, 2013, 11:04 PM
 */

#ifndef HSPLITERCLIENT_H
#define	HSPLITERCLIENT_H

#include "../htdba/htDba.h"

class hSpliterClient
{
public:

	enum Mode
	{
		START,
		CONTINUE
	};
	
	virtual ~hSpliterClient()  { }
	virtual KeyRange getSplit() = 0;
	virtual bool tryKeyCommit(std::string key) = 0;
	virtual void setKeyCommited(std::string key) = 0;
};

#endif	/* HSPLITERCLIENT_H */

