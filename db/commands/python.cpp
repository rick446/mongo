// python.cpp

/**
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "pch.h"
#include "../db.h"
#include "../commands.h"
#include "../../client/dbclient.h"
#include "../../client/connpool.h"
#include "../../client/parallel.h"


#include "../instance.h"
#include "../../scripting/engine.h"
#include "../queryoptimizer.h"
#include "../matcher.h"
#include "../clientcursor.h"
#include "../replutil.h"
#include "../../s/d_chunk_manager.h"
#include "../../s/d_logic.h"

#include <boost/python.hpp>
#include <Python.h>
#include "python.h"


namespace mongo {

  namespace python {

    using namespace boost::python;
    
    Config::Config( const string& _dbname , const BSONObj& cmdObj ) {
      dbname = _dbname;
      ns = dbname + "." + cmdObj.firstElement().valuestr();

      query = cmdObj["query"].embeddedObject();
      map_function = cmdObj["map_function"].String();
    }

    class PythonCommand : public Command {

    public:
      PythonCommand() : Command("python", false, "python") {
        Py_Initialize();
      }
      
      virtual bool slaveOk() const { return !replSet; }
      virtual bool slaveOverrideOk() { return true; }
      
      virtual void help( stringstream &help ) const {
        help << "Run some python\n";
      }

      virtual LockType locktype() const { return NONE; }

      bool run(const string& dbname , BSONObj& cmd, string& errmsg, 
               BSONObjBuilder& result, bool fromRepl ) {
        // Configuration
        Client::GodScope cg;
        Config config( dbname , cmd );

        // traversing collection
        readlock lock( config.ns );        
        Client::Context ctx( config.ns );

        object main_module = import("__main__");
        object main_namespace = main_module.attr("__dict__");
        object bson = import("bson");
        main_namespace.attr("clear")();
        main_namespace["bson"] = bson;
        main_namespace["pymongo"] = import("pymongo");

        exec(config.map_function.c_str(), main_namespace);
        object map_function = main_namespace["map"];
        object wr_stdout = import("sys").attr("stdout").attr("write");

        shared_ptr<Cursor> temp = bestGuessCursor( 
                                                  config.ns.c_str(), 
                                                  config.query, BSONObj() );
        for(
            auto_ptr<ClientCursor> cursor( 
                                          new ClientCursor( QueryOption_NoCursorTimeout , 
                                                            temp , config.ns.c_str() ) );
            cursor->ok();
            cursor->advance()) {
          BSONObj o = cursor->current();
          object o_str = boost::python::str(o.objdata(), o.objsize());
          object o_bson = bson.attr("BSON")(o_str);
          try {
            object map_gen = map_function(o_bson.attr("decode")());
            object map_iter = map_gen.attr("__iter__")();
            while(true) {
              object value =map_iter.attr("next")();
              wr_stdout(value);
              wr_stdout("\n");
            }
          }
          catch(error_already_set const &)
            {
              break;
            }
             // result.append(string(o["_id"]), extract<double>(mr));
        }

        // Building output
        // result.append("python", python);

        return true;
      };

    } pythonCommand; // end of PythonCommand class
    
  }
}
 
