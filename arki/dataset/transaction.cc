/*
 * dataset/index - Dataset index infrastructure
 *
 * Copyright (C) 2007  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/dataset/transaction.h>

using namespace std;
using namespace arki;

namespace arki {
namespace dataset {

Pending::Pending(Transaction* trans) : trans(trans)
{
	trans->ref();
}
Pending::Pending(const Pending& p) : trans(p.trans)
{
	trans->ref();
}
Pending::~Pending()
{
	if (trans && trans->unref())
	{
		trans->rollback();
		delete trans;
	}
}
Pending& Pending::operator=(const Pending& p)
{
	if (p.trans)
		p.trans->ref();
	if (trans && trans->unref())
	{
		trans->rollback();
		delete trans;
	}
	trans = p.trans;
	return *this;
}
void Pending::commit()
{
	if (trans)
	{
		trans->commit();
		if (trans->unref())
			delete trans;
		trans = 0;
	}
}
void Pending::rollback()
{
	if (trans)
	{
		trans->rollback();
		if (trans->unref())
			delete trans;
		trans = 0;
	}
}

}
}
// vim:set ts=4 sw=4:
