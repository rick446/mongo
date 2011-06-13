// mr.cpp

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

#include "sum.h"

namespace mongo {

  namespace sum {

    Config::Config( const string& _dbname , const BSONObj& cmdObj ) {
      dbname = _dbname;
      ns = dbname + "." + cmdObj.firstElement().valuestr();

      query = cmdObj["query"].embeddedObject();
      field = cmdObj["field"].String();
    }

    class SumCommand : public Command {

    public:
      SumCommand() : Command("sum", false, "sum") {}
      
      virtual bool slaveOk() const { return !replSet; }
      virtual bool slaveOverrideOk() { return true; }
      
      virtual void help( stringstream &help ) const {
        help << "Calc a sum\n";
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

        shared_ptr<Cursor> temp = bestGuessCursor( config.ns.c_str(), config.query, BSONObj() );
        auto_ptr<ClientCursor> cursor( 
                                      new ClientCursor( QueryOption_NoCursorTimeout , temp , config.ns.c_str() ) );

        double sum= 0;

        while ( cursor->ok() ) {

          BSONObj o = cursor->current();
          sum += o[config.field].Number();
          cursor->advance();
        }

        // Building output
        result.append("sum", sum);

        return true;
      };

    } sumCommand; // end of SumCommand class
  }
}
 
