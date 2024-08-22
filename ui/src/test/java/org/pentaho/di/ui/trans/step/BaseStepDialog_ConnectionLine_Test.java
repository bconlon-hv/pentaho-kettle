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
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.shared.DatabaseConnectionManager;
import org.pentaho.di.shared.DatabaseManagementInterface;
import org.pentaho.di.shared.MemorySharedObjectsIO;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.database.dialog.DatabaseDialog;
import org.pentaho.di.ui.spoon.Spoon;

import java.util.List;
import java.util.function.Supplier;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.widgets.Shell;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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

  private static String INITIAL_NAME = "qwerty";
  private static String INPUT_NAME = "asdfg";

  private static String INITIAL_HOST = "1.2.3.4";
  private static String INPUT_HOST = "5.6.7.8";

  @BeforeClass
  public static void initKettle() throws Exception {
    KettleEnvironment.init();
    DefaultBowl.getInstance().setSharedObjectsIO( new MemorySharedObjectsIO() );
  }

  @Test
  public void adds_WhenConnectionNameIsUnique() throws Exception {
    TransMeta transMeta = new TransMeta();

    invokeAddConnectionListener( transMeta, INPUT_NAME );

    assertOnlyDbExists( transMeta, INPUT_NAME, INPUT_HOST );
  }

  @Test
  public void ignores_WhenConnectionNameIsUsed() throws Exception {
    TransMeta transMeta = new TransMeta();
    transMeta.getDatabaseManagementInterface().addDatabase( createDefaultDatabase() );

    invokeAddConnectionListener( transMeta, null );

    assertOnlyDbExists( transMeta, INITIAL_NAME, INITIAL_HOST );
  }

  private void invokeAddConnectionListener( TransMeta transMeta, String answeredName ) throws Exception {
    BaseStepDialog dialog = mock( BaseStepDialog.class );
    when( dialog.showDbDialogUnlessCancelledOrValid( anyDbMeta(), any() ) )
      .thenAnswer( new PropsSettingAnswer( answeredName, "TODO" ) );

    Supplier<Spoon> mockSupplier = mock( Supplier.class );
    Spoon mockSpoon = mock( Spoon.class );
    DefaultBowl mockDefaultBowl = mock( DefaultBowl.class );

    Whitebox.setInternalState( dialog, "spoonSupplier", mockSupplier );
    when( mockSupplier.get() ).thenReturn( mockSpoon );
    DatabaseConnectionManager mockDbManager = mock( DatabaseConnectionManager.class );
    doReturn( DefaultBowl.getInstance() ).when( mockSpoon ).getBowl();
    when( mockDefaultBowl.getManager( DatabaseManagementInterface.class ) ).thenReturn( mockDbManager );

    dialog.transMeta = transMeta;
    dialog.new AddConnectionListener( mock( CCombo.class ) ).widgetSelected( null );
    if ( answeredName != null ) {
      verify( mockSpoon, times( 1 ) ).refreshTree( anyString() );
    }
  }

  @Test
  public void edits_globalConnectionWhenNotRenamed() throws Exception {
    //TODO
    TransMeta transMeta = new TransMeta();
    transMeta.getDatabaseManagementInterface().addDatabase( createDefaultDatabase() );

    DatabaseManagementInterface dbMgr =  DefaultBowl.getInstance().getManager( DatabaseManagementInterface.class );

    List<DatabaseMeta> bowlDbs = dbMgr.getDatabases();
    List<DatabaseMeta> transDbs = transMeta.getDatabases();

    invokeAddConnectionListener( transMeta, INITIAL_NAME );
    List<DatabaseMeta> bowlDbs2 = dbMgr.getDatabases();
    List<DatabaseMeta> transDbs2 = transMeta.getDatabases();
    assertEquals( 2, transMeta.getDatabases().size() );
//    assertEquals( name, transMeta.getDatabase( 0 ).getName() );
//    assertEquals( host, transMeta.getDatabase( 0 ).getHostname() );

    invokeEditConnectionListener( transMeta, INITIAL_NAME );
  }

  @Test
  public void edits_localConnectionWhenNotRenamed() throws Exception {
    TransMeta transMeta = new TransMeta();
    transMeta.getDatabaseManagementInterface().addDatabase( createDefaultDatabase() );

    invokeEditConnectionListener( transMeta, INITIAL_NAME );

    assertOnlyDbExists( transMeta, INITIAL_NAME, INPUT_HOST );
  }

  @Test
  public void edits_WhenNewNameIsUnique() throws Exception {
    TransMeta transMeta = new TransMeta();
    transMeta.getDatabaseManagementInterface().addDatabase( createDefaultDatabase() );

    invokeEditConnectionListener( transMeta, INPUT_NAME );

    assertOnlyDbExists( transMeta, INPUT_NAME, INPUT_HOST );
  }

  @Test
  public void ignores_WhenNewNameIsUsed() throws Exception {
    TransMeta transMeta = new TransMeta();
    transMeta.getDatabaseManagementInterface().addDatabase( createDefaultDatabase() );

    invokeEditConnectionListener( transMeta, null );

    assertOnlyDbExists( transMeta, INITIAL_NAME, INITIAL_HOST );
  }

  private void invokeEditConnectionListener( TransMeta transMeta, String answeredName ) throws Exception {
    BaseStepDialog dialog = mock( BaseStepDialog.class );
    when( dialog.showDbDialogUnlessCancelledOrValid( anyDbMeta(), anyDbMeta() ) )
      .thenAnswer( new PropsSettingAnswer( answeredName, INPUT_HOST ) );

    Supplier<Spoon> mockSupplier = mock( Supplier.class );
    Spoon mockSpoon = mock( Spoon.class );
    Whitebox.setInternalState( dialog, "spoonSupplier", mockSupplier );
    when( mockSupplier.get() ).thenReturn( mockSpoon );
    when( mockSpoon.getBowl() ).thenReturn( DefaultBowl.getInstance() );

    CCombo combo = mock( CCombo.class );
    when( combo.getText() ).thenReturn( INITIAL_NAME );

    dialog.transMeta = transMeta;
    dialog.new EditConnectionListener( combo ).widgetSelected( null );
  }


  private DatabaseMeta createDefaultDatabase() {
    DatabaseMeta existing = new DatabaseMeta();
    existing.setName( INITIAL_NAME );
    existing.setHostname( INITIAL_HOST );
    return existing;
  }

  private void assertOnlyDbExists( TransMeta transMeta, String name, String host ) {
    assertEquals( 1, transMeta.getDatabases().size() );
    assertEquals( name, transMeta.getDatabase( 0 ).getName() );
    assertEquals( host, transMeta.getDatabase( 0 ).getHostname() );
  }

  private static DatabaseMeta anyDbMeta() {
    return any( DatabaseMeta.class );
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
    transMeta.getDatabaseManagementInterface().addDatabase( db );

    BaseStepDialog dialog = mock( BaseStepDialog.class );
    dialog.databaseDialog = databaseDialog;
    dialog.transMeta = transMeta;
    when( dialog.showDbDialogUnlessCancelledOrValid( anyDbMeta(), anyDbMeta() ) ).thenCallRealMethod();
    when( dialog.getDatabaseDialog( any() ) ).thenCallRealMethod();

    String result = dialog.showDbDialogUnlessCancelledOrValid( (DatabaseMeta) db.clone(), db );
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
    transMeta.getDatabaseManagementInterface().addDatabase( db1 );
    transMeta.getDatabaseManagementInterface().addDatabase( db2 );

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

    BaseStepDialog dialog = mock( BaseStepDialog.class );
    dialog.databaseDialog = databaseDialog;
    dialog.transMeta = transMeta;
    when( dialog.showDbDialogUnlessCancelledOrValid( anyDbMeta(), anyDbMeta() ) ).thenCallRealMethod();
    when( dialog.getDatabaseDialog( any() ) ).thenCallRealMethod();

    // try to rename db1 ("qwerty")
    String result = dialog.showDbDialogUnlessCancelledOrValid( (DatabaseMeta) db1.clone(), db1 );
    assertEquals( expectedResult, result );

    // database dialog should be shown four times
    verify( databaseDialog, times( 4 ) ).open();
    // and the error message should be shown three times
    verify( dialog, times( 3 ) ).showDbExistsDialog( anyDbMeta() );
  }
}
