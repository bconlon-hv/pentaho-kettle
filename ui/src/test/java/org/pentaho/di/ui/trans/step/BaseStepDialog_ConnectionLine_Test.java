/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.ui.trans.step;

import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.shared.DatabaseManagementInterface;
import org.pentaho.di.shared.MemorySharedObjectsIO;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.database.dialog.DatabaseDialog;
import org.pentaho.di.ui.spoon.Spoon;

import java.util.function.Supplier;

import org.eclipse.swt.custom.CCombo;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

/**
 * @author Andrey Khayrutdinov
 */
public class BaseStepDialog_ConnectionLine_Test {
  @ClassRule
  public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

  private static final String INITIAL_NAME = "qwerty";
  private static final String INPUT_NAME = "asdfg";

  private static final String INITIAL_HOST = "1.2.3.4";
  private static final String INPUT_HOST = "5.6.7.8";

  private static BaseStepDialog mockDialog;
  private static Supplier<Spoon> mockSupplier;
  private static Spoon mockSpoon;
  private static DatabaseManagementInterface dbMgr;

  @BeforeClass
  public static void initKettle() throws Exception {
    KettleEnvironment.init();
    DefaultBowl.getInstance().setSharedObjectsIO( new MemorySharedObjectsIO() );
    DefaultBowl.getInstance().clearManagers();

    mockSupplier = mock( Supplier.class );
    mockSpoon = mock( Spoon.class );
    mockDialog = mock( BaseStepDialog.class );

    Whitebox.setInternalState( mockDialog, "spoonSupplier", mockSupplier );
    when( mockSupplier.get() ).thenReturn( mockSpoon );
    doReturn( DefaultBowl.getInstance() ).when( mockSpoon ).getBowl();

    dbMgr = DefaultBowl.getInstance().getManager( DatabaseManagementInterface.class );
  }

  @After
  public void perTestTeardown() throws KettleException {
    clearInvocations( mockSpoon );
    for ( DatabaseMeta db : dbMgr.getDatabases() ) {
      dbMgr.removeDatabase( db );
    }
  }

  @Test
  public void adds_WhenConnectionNameIsUnique() throws Exception {
    TransMeta transMeta = new TransMeta();

    invokeAddConnectionListener( transMeta, INPUT_NAME );

    assertOnlyOneActiveDb( transMeta, INPUT_NAME, INPUT_HOST );
    assertNumberOfGlobalDBs( 1 );
    assertNumberOfLocalDBs( transMeta, 0 );
  }

  @Test
  public void adds_WhenGlobalConnectionNameOverridesLocal() throws Exception {
    TransMeta transMeta = new TransMeta();

    transMeta.addDatabase( createDefaultDatabase() ); //local
    assertOnlyOneActiveDb( transMeta, INITIAL_NAME, INITIAL_HOST );
    assertTotalDbs( transMeta, 1 );

    invokeAddConnectionListener( transMeta, INITIAL_NAME ); //global
    assertOnlyOneActiveDb( transMeta, INITIAL_NAME, INPUT_HOST );
    assertTotalDbs( transMeta, 2 );
    assertNumberOfGlobalDBs( 1 );
    assertNumberOfLocalDBs( transMeta, 1 );
  }

  private void assertTotalDbs( TransMeta transMeta, int expected ) throws KettleException {
    assertEquals( expected,
      transMeta.getDatabaseManagementInterface().getDatabases().size() + dbMgr.getDatabases().size() );
  }

  private void assertNumberOfGlobalDBs( int expected ) throws KettleException {
    assertEquals( expected, dbMgr.getDatabases().size() );
  }

  private void assertNumberOfLocalDBs( TransMeta transMeta, int expected ) throws KettleException {
    assertEquals( expected, transMeta.getDatabaseManagementInterface().getDatabases().size() );
  }

  @Test
  public void ignoresAdd_WhenConnectionNameIsNull() throws Exception {
    TransMeta transMeta = new TransMeta();
    dbMgr.addDatabase( createDefaultDatabase() );

    invokeAddConnectionListener( transMeta, null );

    assertOnlyOneActiveDb( transMeta, INITIAL_NAME, INITIAL_HOST );
    assertTotalDbs( transMeta, 1 );
  }

  private void invokeAddConnectionListener( TransMeta transMeta, String answeredName ) throws Exception {
    when( mockDialog.showDbDialogUnlessCancelledOrValid( anyDbMeta(), any(), anyDbMgr() ) )
      .thenAnswer( new PropsSettingAnswer( answeredName, INPUT_HOST ) );

    mockDialog.transMeta = transMeta;
    mockDialog.new AddConnectionListener( mock( CCombo.class ) ).widgetSelected( null );
    if ( answeredName != null ) {
      verify( mockSpoon, times( 1 ) ).refreshTree( anyString() );
    }
  }

  @Test
  public void edits_globalConnectionWhenNotRenamed() throws Exception {
    TransMeta transMeta = new TransMeta();
    DatabaseMeta db = createDefaultDatabase();

    transMeta.addDatabase( db );
    dbMgr.addDatabase( db );
    assertTotalDbs( transMeta, 2 );
    assertNumberOfGlobalDBs( 1 );
    assertNumberOfLocalDBs( transMeta, 1 );

    invokeEditConnectionListener( transMeta, INITIAL_NAME, dbMgr );

    DatabaseMeta localDb = transMeta.getDatabaseManagementInterface().getDatabase( INITIAL_NAME );
    DatabaseMeta globalDb =
      transMeta.getDatabases().stream().filter( databaseMeta -> databaseMeta.getName().equals( INITIAL_NAME ) )
        .findFirst().get();

    assertEquals( INITIAL_HOST, localDb.getHostname() );
    assertEquals( INPUT_HOST, globalDb.getHostname() );
    assertTotalDbs( transMeta, 2 );
    assertNumberOfGlobalDBs( 1 );
    assertNumberOfLocalDBs( transMeta, 1 );
  }

  @Test
  public void edits_localConnectionWhenNotRenamed() throws Exception {
    TransMeta transMeta = new TransMeta();
    DatabaseManagementInterface transDbMgr = transMeta.getDatabaseManagementInterface();
    transDbMgr.addDatabase( createDefaultDatabase() );

    invokeEditConnectionListener( transMeta, INITIAL_NAME, transDbMgr );

    assertOnlyOneActiveDb( transMeta, INITIAL_NAME, INPUT_HOST );
    assertTotalDbs( transMeta, 1 );
    assertNumberOfGlobalDBs( 0 );
    assertNumberOfLocalDBs( transMeta, 1 );
  }

  @Test
  public void edits_GlobalConnectionWhenNewNameIsUnique() throws Exception {
    TransMeta transMeta = new TransMeta();
    dbMgr.addDatabase( createDefaultDatabase() );

    invokeEditConnectionListener( transMeta, INPUT_NAME, dbMgr );

    assertOnlyOneActiveDb( transMeta, INPUT_NAME, INPUT_HOST );
    assertTotalDbs( transMeta, 1 );
    assertNumberOfGlobalDBs( 1 );
    assertNumberOfLocalDBs( transMeta, 0 );
  }

  @Test
  public void ignores_EditToLocalConnectionWhenNewNameIsNull() throws Exception {
    TransMeta transMeta = new TransMeta();
    DatabaseManagementInterface transDbMgr = transMeta.getDatabaseManagementInterface();
    transDbMgr.addDatabase( createDefaultDatabase() );

    invokeEditConnectionListener( transMeta, null, transDbMgr );

    assertOnlyOneActiveDb( transMeta, INITIAL_NAME, INITIAL_HOST );
    assertTotalDbs( transMeta, 1 );
    assertNumberOfGlobalDBs( 0 );
    assertNumberOfLocalDBs( transMeta, 1 );
  }

  @Test
  public void ignores_EditToGlobalConnectionWhenNewNameIsNull() throws Exception {
    TransMeta transMeta = new TransMeta();
    dbMgr.addDatabase( createDefaultDatabase() );

    invokeEditConnectionListener( transMeta, null, dbMgr );

    assertOnlyOneActiveDb( transMeta, INITIAL_NAME, INITIAL_HOST );
    assertTotalDbs( transMeta, 1 );
    assertNumberOfGlobalDBs( 1 );
    assertNumberOfLocalDBs( transMeta, 0 );
  }

  private void invokeEditConnectionListener( TransMeta transMeta, String answeredName,
                                             DatabaseManagementInterface dbMgr ) throws Exception {
    when( mockDialog.showDbDialogUnlessCancelledOrValid( anyDbMeta(), anyDbMeta(), anyDbMgr() ) )
      .thenAnswer( new PropsSettingAnswer( answeredName, INPUT_HOST ) );

    CCombo combo = mock( CCombo.class );
    when( combo.getText() ).thenReturn( INITIAL_NAME );

    mockDialog.transMeta = transMeta;
    mockDialog.new EditConnectionListener( combo ).widgetSelected( null );
  }

  private DatabaseMeta createDefaultDatabase() {
    DatabaseMeta existing = new DatabaseMeta();
    existing.setName( INITIAL_NAME );
    existing.setHostname( INITIAL_HOST );
    return existing;
  }

  private void assertOnlyOneActiveDb( TransMeta transMeta, String name, String host ) {
    assertEquals( 1, transMeta.getDatabases().size() );
    assertEquals( name, transMeta.getDatabase( 0 ).getName() );
    assertEquals( host, transMeta.getDatabase( 0 ).getHostname() );
  }

  private static DatabaseMeta anyDbMeta() {
    return any( DatabaseMeta.class );
  }

  //TODO add the new tests from job entry equivalent

  private static DatabaseManagementInterface anyDbMgr() {
    return any( DatabaseManagementInterface.class );
  }

  private static class PropsSettingAnswer implements Answer<String> {
    private final String name;
    private final String host;

    public PropsSettingAnswer( String name, String host ) {
      this.name = name;
      this.host = host;
    }

    @Override
    public String answer( InvocationOnMock invocation ) throws Throwable {
      DatabaseMeta meta = (DatabaseMeta) invocation.getArguments()[ 0 ];
      meta.setName( name );
      meta.setHostname( host );
      return name;
    }
  }


  @Test
  public void showDbDialog_ReturnsNull_OnCancel() throws Exception {
    // null as input emulates cancelling
    test_showDbDialogUnlessCancelledOrValid_ShownOnce( null, null );
  }

  @Test
  public void showDbDialog_ReturnsInputName_WhenItIsUnique() throws Exception {
    test_showDbDialogUnlessCancelledOrValid_ShownOnce( INPUT_NAME, INPUT_NAME );
  }

  @Test
  public void showDbDialog_ReturnsInputName_WhenItIsUnique_WithSpaces() throws Exception {
    String input = " " + INPUT_NAME + " ";
    test_showDbDialogUnlessCancelledOrValid_ShownOnce( input, INPUT_NAME );
  }

  @Test
  public void showDbDialog_ReturnsExistingName_WhenNameWasNotChanged() throws Exception {
    // this is the case of editing when name was not changed (e.g., host was updated)
    test_showDbDialogUnlessCancelledOrValid_ShownOnce( INITIAL_NAME, INITIAL_NAME );
  }

  private void test_showDbDialogUnlessCancelledOrValid_ShownOnce( String inputName,
                                                                  String expectedResult ) throws Exception {
    DatabaseDialog databaseDialog = mock( DatabaseDialog.class );
    when( databaseDialog.open() ).thenReturn( inputName );
    when( databaseDialog.getDatabaseMeta() ).thenReturn( createDefaultDatabase() );

    TransMeta transMeta = new TransMeta();
    DatabaseMeta db = createDefaultDatabase();
    dbMgr.addDatabase( db );

    mockDialog.databaseDialog = databaseDialog;
    mockDialog.transMeta = transMeta;
    when( mockDialog.showDbDialogUnlessCancelledOrValid( anyDbMeta(), anyDbMeta(), anyDbMgr() ) ).thenCallRealMethod();
    when( mockDialog.getDatabaseDialog( any() ) ).thenCallRealMethod();

    String result = mockDialog.showDbDialogUnlessCancelledOrValid( (DatabaseMeta) db.clone(), db, dbMgr );
    assertEquals( expectedResult, result );

    // database dialog should be shown only once
    verify( databaseDialog, times( 1 ) ).open();
  }

  @Test
  public void showDbDialog_LoopsUntilUniqueValueIsInput() throws Exception {
    DatabaseMeta db1 = createDefaultDatabase();

    DatabaseMeta db2 = createDefaultDatabase();
    db2.setName( INPUT_NAME );

    TransMeta transMeta = new TransMeta();
    dbMgr.addDatabase( db1 );
    dbMgr.addDatabase( db2 );

    final String expectedResult = INPUT_NAME + "2";

    DatabaseDialog databaseDialog = mock( DatabaseDialog.class );
    when( databaseDialog.open() )
      // duplicate
      .thenReturn( INPUT_NAME )
      // duplicate with spaces
      .thenReturn( INPUT_NAME + " " )
      // duplicate in other case
      .thenReturn( INPUT_NAME.toUpperCase() )
      // unique value
      .thenReturn( expectedResult );

    when( databaseDialog.getDatabaseMeta() ).thenReturn( createDefaultDatabase() );

    mockDialog.databaseDialog = databaseDialog;
    mockDialog.transMeta = transMeta;
    when( mockDialog.showDbDialogUnlessCancelledOrValid( anyDbMeta(), anyDbMeta(), anyDbMgr() ) ).thenCallRealMethod();
    when( mockDialog.getDatabaseDialog( any() ) ).thenCallRealMethod();

    // try to rename db1 ("qwerty")
    String result = mockDialog.showDbDialogUnlessCancelledOrValid( (DatabaseMeta) db1.clone(), db1, dbMgr );
    assertEquals( expectedResult, result );

    // database dialog should be shown four times
    verify( databaseDialog, times( 4 ) ).open();
    // and the error message should be shown three times
    verify( mockDialog, times( 3 ) ).showDbExistsDialog( anyDbMeta() );
  }
}
