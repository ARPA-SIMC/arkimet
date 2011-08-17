/*
 * dataset/discard - Dataset that just discards all data
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/dataset/discard.h>
#include <arki/metadata.h>
#include <arki/configfile.h>
#include <arki/types/assigneddataset.h>

using namespace std;

namespace arki {
namespace dataset {

Discard::Discard(const ConfigFile& cfg)
{
	m_name = cfg.value("name");
}

Discard::~Discard()
{
}

WritableDataset::AcquireResult Discard::acquire(Metadata& md)
{
	md.set(types::AssignedDataset::create(m_name, ""));
	return ACQ_OK;
}

bool Discard::replace(Metadata& md)
{
	md.set(types::AssignedDataset::create(m_name, ""));
	return true;
}

WritableDataset::AcquireResult Discard::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
	out << "Resetting dataset information to mark that the message has been discarded" << endl;
	return ACQ_OK;
}

}
}
// vim:set ts=4 sw=4:
