/*
 * arki-config - Configuration wizard
 *
 * Copyright (C) 2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <arki/configfile.h>
#include <arki/dataset/targetfile.h>
#include <arki/dataset/index/base.h>
#include <arki/matcher.h>
#include <arki/runtime.h>

#include "config.h"

#include <map>
#include <cstdio>

#ifdef HAVE_LIBREADLINE
 #if defined(HAVE_READLINE_READLINE_H)
  #include <readline/readline.h>
 #elif defined(HAVE_READLINE_H)
  #include <readline.h>
 #else /* !defined(HAVE_READLINE_H) */
  extern char *readline ();
 #endif /* !defined(HAVE_READLINE_H) */
 char *cmdline = NULL;
#else /* !defined(HAVE_READLINE_READLINE_H) */
 /* no readline */
#endif /* HAVE_LIBREADLINE */


//#define debug(...) fprintf(stderr, __VA_ARGS__)
#define debug(...) do{}while(0)

using namespace std;
using namespace arki;
using namespace wibble;
using namespace wibble::sys;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	VectorOption<String>* cfgfiles;
	BoolOption* fix;
	BoolOption* repack;
	BoolOption* invalidate;
	BoolOption* stats;
	StringOption* salvage;
	StringOption* op_remove;

	Options() : StandardParserWithManpage("arki-config", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[dataset directory]";
		description = "Text-mode configuration wizard for arkimet datasets.";
	}
};

}
}

// Ask a yes/no question
bool ask_bool(const std::string& prompt, bool def = false)
{
	string full_prompt = prompt;
	if (def)
		full_prompt += " (Y/n) ";
	else
		full_prompt += " (y/N) ";

	while (true)
	{
		char* rl_answer = readline(full_prompt.c_str());
		string answer = rl_answer;
		free(rl_answer);

		answer = str::tolower(str::trim(answer));
		if (answer.empty())
			return def;

		if (answer == "y") return true;
		if (answer == "n") return false;
	}
}

// Ask a string
string ask_string(const std::string& prompt, const std::string& def = std::string())
{
	string full_prompt = prompt;
	if (not def.empty())
		full_prompt += " (default: " + def + ") ";
	else
		full_prompt += " ";

	char* rl_answer = readline(full_prompt.c_str());
	string answer = rl_answer;
	free(rl_answer);

	answer = str::trim(answer);
	if (answer.empty())
		return def;
	return answer;
}

template<typename ENTRY> char ask_choice_shortcut(const ENTRY& e) { return 0; }
template<typename ENTRY> std::string ask_choice_description(const ENTRY& e) { return str::fmt(e); }

struct AnnotatedChoice
{
	string value;
	string description;
	AnnotatedChoice() {}
	AnnotatedChoice(const std::string& value, const std::string& description)
		: value(value), description(description) {}
	bool operator==(const AnnotatedChoice& ac) const { return value == ac.value; }
};
std::string ask_choice_description(const AnnotatedChoice& e) { return e.value + ": " + e.description; }

template<typename ENTRY>
ENTRY ask_choice(const std::string& prompt, const std::vector<ENTRY>& entries, ENTRY def = ENTRY())
{
	while (true)
	{
		cout << " * Menu:" << endl;
		cout << endl;

		map<string, ENTRY> entry_map;
		string def_choice;
		for (size_t i = 0; i < entries.size(); ++i)
		{
			string key;
			char shortcut = ask_choice_shortcut(entries[i]);
			if (shortcut)
				key = shortcut;
			else
				key = str::fmt(i + 1);
			cout << " " << key << ": ";

			if (entries[i] == def)
				def_choice = key;

			cout << ask_choice_description(entries[i]) << endl;

			entry_map.insert(make_pair(key, entries[i]));
		}

		cout << endl;
		string res = ask_string(prompt, def_choice);
		typename map<string, ENTRY>::iterator i = entry_map.find(res);
		if (i != entry_map.end())
			return i->second;
		cout << "Sorry, I do not understand your choice: please try again." << endl;
		cout << endl;
	}
}

class Wizard;

/**
 * Interface for actions performed by the wizard
 */
class Action
{
protected:
	Wizard& w;

public:
	char shortcut;

	Action(Wizard& w) : w(w), shortcut(0) {}
	virtual ~Action() {}

	virtual bool enabled() const { return true; }
	virtual std::string description() const = 0;
	virtual void activate() = 0;
};
char ask_choice_shortcut(Action* e) { return e->shortcut; }
std::string ask_choice_description(Action* e) { return e->description(); }

/**
 * Configuration wizard
 */
struct Wizard
{
	ConfigFile cfg;
	ostream& out;
	bool finished;
	string datasetName;
	string configFile;
	string absConfigFile;
	map<types::Code, string> md_descs;

	vector<Action*> actions;

	Wizard(commandline::Options& opts) : out(cout), finished(false)
   	{
		// Initialise the metadata descriptions
		md_descs[types::TYPE_ORIGIN] = "description of the source of the data";
		md_descs[types::TYPE_PRODUCT] = "product type";
		md_descs[types::TYPE_LEVEL] = "vertical level";
		md_descs[types::TYPE_TIMERANGE] = "time range of statistical aggregation";
		md_descs[types::TYPE_REFTIME] = "reference time";
		md_descs[types::TYPE_AREA] = "geographical area";
		md_descs[types::TYPE_ENSEMBLE] = "description of the ensemble forecast run";
		md_descs[types::TYPE_RUN] = "identification of the forecast run within a day";
		//md_descs[types::TYPE_BBOX] = "bounding box";

		if (opts.hasNext())
		{
			string path = opts.next();
			if (path == "config" or str::endsWith(path, "/config"))
				configFile = path;
			else
				configFile = str::joinpath(path, "config");
		} else
			configFile = "./config";

		absConfigFile = fs::abspath(configFile);
		datasetName = absConfigFile.substr(0, absConfigFile.size() - 7);
		size_t pos = datasetName.rfind("/");
		if (pos == string::npos)
			throw wibble::exception::Consistency("computing dataset name", "config file " + absConfigFile + " cannot be in the root directory");
		datasetName = datasetName.substr(pos + 1);
	}

	~Wizard()
	{
		for (vector<Action*>::iterator i = actions.begin();
				i != actions.end(); ++i)
			delete *i;
	}

	void readConfig()
	{
		if (!fs::access(configFile, F_OK))
		{
			if (!ask_bool("Config file " + configFile + " does not exist: should I create it?", true))
			{
				out << "Ok, nothing to do, then." << endl;
				finished = true;
			} else {
				// Generate a basic config
				cfg.setValue("step", "daily");
				cfg.setValue("type", "local");
				if (datasetName != "error" and datasetName != "duplicates")
				{
					cfg.setValue("unique", "reftime, timerange, product, level, area");
					cfg.setValue("index", "reftime, timerange, product, level");
				}
			}
		} else {
			runtime::parseConfigFile(cfg, configFile);
		}

		makeActions();
	}

	void makeActions();

	Action* menu()
	{
		out << endl;
		out << " * Configuration for dataset [" + datasetName + "] in " + configFile << endl;
		out << endl;
		cfg.output(out, "(standard output)");
		out << endl;

		vector<Action*> enabled_actions;
		for (vector<Action*>::const_iterator i = actions.begin();
				i != actions.end(); ++i)
			if ((*i)->enabled())
				enabled_actions.push_back(*i);

		return ask_choice("Please choose an option to configure:", enabled_actions);
	}

	void main_loop()
	{
		while (!finished)
		{
			Action* a = menu();
			a->activate();
		}
	}

	void save()
	{
		runtime::Output output(configFile);
		cfg.output(output.stream(), output.name());
		out << "Configuration saved in " << configFile << "." << endl;
	}
};

class QuitAction : public Action
{
public:
	QuitAction(Wizard& w) : Action(w)
   	{
		shortcut = 'q';
	}
	virtual std::string description() const
	{
		return "Save and quit.";
	}
	virtual void activate()
	{
		w.finished = true;
	}
};

/**
 * Configure dataset step
 */
class StepAction : public Action
{
public:
	StepAction(Wizard& state) : Action(state) {}
	virtual std::string description() const
	{
		return "step: Change the granularity of data aggregation.";
	}
	virtual void activate()
	{
		string cur = str::tolower(w.cfg.value("step"));
		vector<string> steps = dataset::TargetFile::stepList();
		w.cfg.setValue("step",
				ask_choice(
					"Please choose a step for the dataset '" + w.datasetName + "':",
				   	steps, cur));
	}
};

class TypeAction : public Action
{
public:
	TypeAction(Wizard& state) : Action(state) {}
	virtual std::string description() const
	{
		return "type: Change how data is stored on disk.";
	}
	virtual void activate()
	{
		string cur = str::tolower(w.cfg.value("type"));
		AnnotatedChoice cur_choice;
		vector<AnnotatedChoice> types;
		types.push_back(AnnotatedChoice("local", "data, metadata, summaries and indices"));
		if (cur == types.back().value) cur_choice = types.back();
		types.push_back(AnnotatedChoice("outbound", "data only"));
		if (cur == types.back().value) cur_choice = types.back();
		types.push_back(AnnotatedChoice("discard", "nothing is written: all the data is discarded"));
		if (cur == types.back().value) cur_choice = types.back();
		w.cfg.setValue("type",
				ask_choice(
					"Please choose a type for the dataset '" + w.datasetName + "':",
				   	types, cur_choice).value);
	}
};

class ReplaceAction : public Action
{
public:
	ReplaceAction(Wizard& state) : Action(state) {}
	virtual bool enabled() const
	{
		string type = str::tolower(w.cfg.value("type"));
		// FIXME: 'test' is deprecated, and is here only for backward
		// compatibility
		return type == "local" || type == "test";
	}
	virtual std::string description() const
	{
		if (ConfigFile::boolValue(w.cfg.value("replace")))
			return "replace: Set duplicates to overwrite the old values (currently they are are rejected).";
		else
			return "replace: Set duplicates to be rejected (currently they overwrite their old values).";
	}
	virtual void activate()
	{
		if (ConfigFile::boolValue(w.cfg.value("replace")))
			w.cfg.setValue("replace", string());
		else
			w.cfg.setValue("replace", "yes");
	}
};

class MDListAction : public Action
{
	std::string key;
	std::string desc;
	vector<types::Code> metadatas;

public:
	MDListAction(Wizard& state, const std::string& key, const std::string& desc)
	   	: Action(state), key(key), desc(desc)
   	{
		metadatas.push_back(types::TYPE_ORIGIN);
    	metadatas.push_back(types::TYPE_PRODUCT);
    	metadatas.push_back(types::TYPE_LEVEL);
    	metadatas.push_back(types::TYPE_TIMERANGE);
    	metadatas.push_back(types::TYPE_REFTIME);
    	metadatas.push_back(types::TYPE_AREA);
    	metadatas.push_back(types::TYPE_ENSEMBLE);
    	//metadatas.push_back(types::TYPE_BBOX);
    	metadatas.push_back(types::TYPE_RUN);
	}

	virtual bool enabled() const
	{
		string type = str::tolower(w.cfg.value("type"));
		// FIXME: 'test' is deprecated, and is here only for backward
		// compatibility
		return type == "local" || type == "test";
	}
	virtual std::string description() const
	{
		return desc;
	}
	virtual void activate()
	{
		string cur = str::tolower(w.cfg.value(key));
		AnnotatedChoice end("quit", "No more changes to perform");

		while (true)
		{
			std::set<types::Code> codes = dataset::index::parseMetadataBitmask(cur);
			vector<AnnotatedChoice> types;
			for (vector<types::Code>::const_iterator i = metadatas.begin(); i != metadatas.end(); ++i)
				if (codes.find(*i) == codes.end())
					types.push_back(AnnotatedChoice(str::tolower(formatCode(*i)), w.md_descs[*i]));
				else
					types.push_back(AnnotatedChoice("*" + str::tolower(formatCode(*i)), w.md_descs[*i]));
			types.push_back(end);

			string sel = ask_choice(
						"Please choose a data type to add or remove:",
						types, end).value;
			if (sel[0] == '*') sel = sel.substr(1);
			if (sel == "quit")
				break;
			types::Code c = types::parseCodeName(sel);
			if (c == types::TYPE_INVALID)
				continue;

			if (codes.find(c) == codes.end())
				codes.insert(c);
			else
				codes.erase(c);

			cur = string();
			for (set<types::Code>::const_iterator i = codes.begin(); i != codes.end(); ++i)
			{
				if (!cur.empty())
					cur += ", ";
				cur += str::tolower(formatCode(*i));
			}
		}
		w.cfg.setValue(key, cur);
	}
};

class FilterAction : public Action
{
	map<types::Code, string> filter_help;
	vector<AnnotatedChoice> types;
	vector<string> matchers;
	AnnotatedChoice end;

	string extract_subexpr(const Matcher& m, types::Code c, const std::string& def = std::string())
	{
		if (m.m_impl)
		{
			matcher::AND::const_iterator i = m.m_impl->find(c);
			if (i != m.m_impl->end())
			{
				string res = i->second->toString();
				size_t pos = res.find(":");
				res = str::trim(res.substr(pos+1));
				return res;
			}
		}
		return def;
	}

public:
	FilterAction(Wizard& state) : Action(state), end("quit", "No more changes to perform")
   	{
		filter_help[types::TYPE_ORIGIN] = 
			"Syntax:\n"
			" - GRIB1,centre,subcentre,process\n"
			" - GRIB2,centre,subcentre,processtype,bgprocessid,processid\n"
			" - BUFR,centre,subcentre\n"
			"Examples:\n"
			" - GRIB1\n"
			" - GRIB1,98\n"
			" - GRIB1,98,2,3\n"
			" - GRIB2,98,2\n"
			" - GRIB2,98,1,2,,4\n"
			" - BUFR,98,1\n"
			;
    	filter_help[types::TYPE_PRODUCT] = 
			"Syntax:\n"
			" - GRIB1,centre,table,product\n"
			" - GRIB2,centre,discipline,category,number\n"
			" - BUFR,type,subtype,localsubtype\n"
			"Examples:\n"
			" - GRIB1\n"
			" - GRIB1,98\n"
			" - GRIB1,98,128,130\n"
			" - GRIB2,98,2\n"
			" - GRIB2,98,1,2\n"
			" - BUFR,1,,0\n"
			;
    	filter_help[types::TYPE_LEVEL] = 
			"Syntax:\n"
			" - GRIB1,type,l1,l2\n"
			" - GRIB2S,type,scale,value\n"
			" - GRIB2D,type1,scale1,value1,type2,scale2,value2\n"
			"Examples:\n"
			" - GRIB1,1\n"
			" - GRIB1,105,2\n"
			" - GRIB1,110,12,13\n"
			" - GRIB2S,110,12,13\n"
			" - GRIB2D,110,12,13,114,15,16\n"
			;
    	filter_help[types::TYPE_TIMERANGE] = 
			"Syntax:\n"
			" - GRIB1,type,p1,p2\n"
			"   p1 and p2 are numbers with units, for example: 30s, 6h\n"
		    "   The units are: s(econd), m(inute), h(our), d(ay), mo(nth), y(ear)\n"
			" - GRIB2,type,unit,p1,p1\n"
			"Examples:\n"
			" - GRIB1,0,0,0\n"
			" - GRIB1,0,6h\n"
			" - GRIB1,2,0,6h\n"
			" - GRIB2,1,2,3,4\n"
			;
    	filter_help[types::TYPE_REFTIME] = 
			"Syntax:\n"
			" - {>=|>|<=|<|=} Date: match the absolute time\n"
			"   '=2009-01-20 12:00:00'\n"
			"   It can be truncated at any point: '<=2009-01-20, =2009'\n"
			"   'today', 'yesterday' and 'tomorrow' work:\n"
		    "   '=today', '>=yesterday, <=tomorrow'\n"
			"   It can be relative: '>1 year before today, <= 3 weeks after 2009-01-20'\n"
			"\n"
			" - {>=|>|<=|<|=} Time: match the time in any day\n"
			"   '<= 12:00:00' (any day before midday)\n"
			"   '>=6, <=12' (between 6:00 and 13:00, any day)\n"
			"\n"
			" - every NN {hour|min|sec}: match exact times within a day\n"
			"   'every 6 hours' (at 0:00, 6:00, 12:00 or 18:00)\n"
			"   It can be appended to a date or time:\n"
			"    - '=2009 every 6 hours'\n"
			"    - '>= 13:30 every hour' (at 13:30 or 19:30)\n"
			"\n"
			" - many expression separated by comma are matched together, and must\n"
		    "   all be satisfied:\n"
			"   '>=2005,<2009,every 6 hours'\n"
			"Examples:\n"
			" - '=yesterday'\n"
			" - '>= 2008-12, <=2009-01'\n"
			" - '>= 1 year ago, every 6 hours'\n"
			" - '>= 3 weeks before today, < tomorrow'\n"
			;
    	filter_help[types::TYPE_AREA] = 
			"Syntax:\n"
			" - style:key=val[,key=val[,...]]\n"
			" - style can be only GRIB, for now.\n"
			" - key and val match the values set by the scanning script.\n"
			"Examples:\n"
			" - GRIB:latfirst=100000,lonfirst=100000\n"
			" - GRIB:Ni=1000,Nj=1000\n"
			;
    	filter_help[types::TYPE_ENSEMBLE] = 
			"Syntax:\n"
			" - style:key=val[,key=val[,...]]\n"
			" - style can be only GRIB, for now.\n"
			" - key and val match the values set by the scanning script.\n"
			"Examples:\n"
			" - GRIB:ld=11,mt=10,nn=12\n"
			" - GRIB:ty=1,pf=5,tf=32\n"
			;
    	filter_help[types::TYPE_RUN] = 
			"Syntax:\n"
			" - MINUTE,hour[:min]\n"
			"Examples:\n"
			" - MINUTE,12\n"
			" - MINUTE,18:30\n"
			;
		matchers = matcher::MatcherType::matcherNames();
		for (vector<string>::const_iterator i = matchers.begin();
				i != matchers.end(); ++i)
		{
			matcher::MatcherType* info = matcher::MatcherType::find(*i);
			types.push_back(AnnotatedChoice(str::tolower(formatCode(info->code)), w.md_descs[info->code]));
		}
		types.push_back(end);
	}

	virtual bool enabled() const
	{
		string type = str::tolower(w.cfg.value("type"));
		// FIXME: 'test' is deprecated, and is here only for backward
		// compatibility
		return type == "local" || type == "test";
	}
	virtual std::string description() const
	{
		return "filter: Change what kind of data goes in this dataset.";
	}

	string validate_subexpr(const matcher::MatcherType& t, const std::string& expr)
	{
		try
		{
			matcher::Implementation* i = t.parse_func(expr);
			if (!i)
				return "parse error";
			delete i;
			return string();
		} catch (wibble::exception::Generic& g) {
			return g.desc();
		} catch (std::exception& e) {
			return e.what();
		}
	}
	static const char* rl_prefill_text;
	static int rl_prefill()
	{
		rl_insert_text(rl_prefill_text);
		return 0;
	}

	std::string ask_subexpr(const matcher::MatcherType& t, std::string val)
	{
		// TODO: show available aliases
		// TODO: allow ? for help
		w.out << filter_help[t.code];
		string prompt = "Match expression for " + str::tolower(t.name) + ": ";
		while (true)
		{
			rl_startup_hook = rl_prefill;
			rl_prefill_text = val.c_str();
			char* rl_answer = readline(prompt.c_str());
			string answer = rl_answer;
			free(rl_answer);
			rl_startup_hook = 0;

			answer = str::trim(answer);
			if (answer.empty())
				return answer;
			string err = validate_subexpr(t, answer);
			if (err.empty())
				return answer;
			w.out << "Error: " + err + ": please try again." << endl;
		}
	}
	virtual void activate()
	{
		string cur = str::tolower(w.cfg.value("filter"));
		while (true)
		{
			// Show a pretty-print of the current filter
			w.out << "Current filter:" << endl;
			Matcher m = Matcher::parse(cur);
			for (vector<string>::const_iterator i = matchers.begin();
					i != matchers.end(); ++i)
			{
				matcher::MatcherType* info = matcher::MatcherType::find(*i);
				string subexpr = extract_subexpr(m, info->code, "(any)");
				w.out << *i << ": " << subexpr << endl;
			}

			// Pick a metadata type to edit
			string sel = ask_choice(
						"Please choose a filter type to edit:",
						types, end).value;
			if (sel == "quit")
				break;
			types::Code c = types::parseCodeName(sel);
			if (c == types::TYPE_INVALID) continue;

			matcher::MatcherType* info = matcher::MatcherType::find(str::tolower(formatCode(c)));
			if (!info) continue;
			
			// Invoke the editor
			string subexpr = extract_subexpr(m, c);
			subexpr = ask_subexpr(*info, subexpr);
			
			// Rebuild the filter expression
			cur.clear();
			for (vector<string>::const_iterator i = matchers.begin();
					i != matchers.end(); ++i)
			{
				matcher::MatcherType* info = matcher::MatcherType::find(*i);
				string expr;
				if (info->code == c)
					expr = subexpr;
				else
					expr = extract_subexpr(m, info->code);

				if (!expr.empty())
				{
					if (!cur.empty())
						cur += "; ";
					cur += *i + ":" + expr;
				}
			}
		}
		w.cfg.setValue("filter", cur);
	}
};
const char* FilterAction::rl_prefill_text = 0;


void Wizard::makeActions()
{
	bool normal = (datasetName != "error" and datasetName != "duplicates");
	if (normal) actions.push_back(new FilterAction(*this));
	if (normal) actions.push_back(new MDListAction(*this, "index", "index: Choose what is indexed to speed up queries"));
	if (normal) actions.push_back(new ReplaceAction(*this));
	actions.push_back(new StepAction(*this));
	actions.push_back(new TypeAction(*this));
	if (normal) actions.push_back(new MDListAction(*this, "unique", "unique: Choose what is significant in telling if two values differ"));
	actions.push_back(new QuitAction(*this));
}

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		// Fill in the information for the wizard 
		Wizard wizard(opts);

		wizard.readConfig();

		wizard.main_loop();

		wizard.save();
	} catch (wibble::exception::BadOption& e) {
		cerr << e.desc() << endl;
		opts.outputHelp(cerr);
		return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}

	return 0;
}

// vim:set ts=4 sw=4:
