/*
 * Copyright (C) 2007,2008,2009  Enrico Zini <enrico@enricozini.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/dataset/test-utils.h>
#include <arki/dataset/ondisk/writer/directory.h>
#include <arki/dataset/ondisk/writer/datafile.h>
#include <arki/dataset/ondisk/reader.h>
#include <arki/metadata.h>
#include <arki/configfile.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset::ondisk;
using namespace arki::dataset::ondisk::writer;
using namespace arki::utils::files;

struct arki_dataset_ondisk_writer_directory_shar {
	ConfigFile cfg;

	arki_dataset_ondisk_writer_directory_shar()
	{
		system("rm -rf testdir");

		cfg.setValue("path", "testdir");
		cfg.setValue("name", "test");
		cfg.setValue("step", "daily");
		cfg.setValue("index", "origin, reftime");
		cfg.setValue("unique", "origin, reftime");
	}
};
TESTGRP(arki_dataset_ondisk_writer_directory);

// TODO
template<> template<>
void to::test<1>()
{
}

}

// vim:set ts=4 sw=4:
