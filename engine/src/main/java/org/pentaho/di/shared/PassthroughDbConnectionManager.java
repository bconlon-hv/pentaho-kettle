/*!
 * Copyright 2024 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.pentaho.di.shared;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.w3c.dom.Node;

/**
 * Database Manager that does not cache anything. Complete passthrough to the provided SharedObjectsIO instance.
 *
 */
public class PassthroughDbConnectionManager implements DatabaseManagementInterface {

  private final SharedObjectsIO sharedObjectsIO;

  public PassthroughDbConnectionManager( SharedObjectsIO sharedObjectsIO ) {
    this.sharedObjectsIO = sharedObjectsIO;
  }

  @Override
  public void addDatabase( DatabaseMeta databaseMeta ) throws KettleException {
    Node node = DatabaseConnectionManager.toNode( databaseMeta );
    sharedObjectsIO.saveSharedObject( DatabaseConnectionManager.DB_TYPE, databaseMeta.getName(), node );
  }

  @Override
  public void removeDatabase( DatabaseMeta databaseMeta ) throws KettleException {
    sharedObjectsIO.delete( DatabaseConnectionManager.DB_TYPE, databaseMeta.getName() );
  }

  @Override
  public void clear() throws KettleException {
    sharedObjectsIO.clear( DatabaseConnectionManager.DB_TYPE );
  }

  @Override
  public List<DatabaseMeta> getDatabases() throws KettleException {
    Map<String, Node> nodeMap = sharedObjectsIO.getSharedObjects( DatabaseConnectionManager.DB_TYPE );
    List<DatabaseMeta> result = new ArrayList<>( nodeMap.size() );

    for ( Node node : nodeMap.values() ) {
      result.add( new DatabaseMeta( node ) );
    }
    return result;
  }

  @Override
  public DatabaseMeta getDatabase( String name ) throws KettleException {
    Node node = sharedObjectsIO.getSharedObject( DatabaseConnectionManager.DB_TYPE, name );
    if ( node == null ) {
      return null;
    }
    return new DatabaseMeta( node );
  }

}
