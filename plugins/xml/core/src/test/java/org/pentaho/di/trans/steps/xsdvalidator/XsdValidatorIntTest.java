/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.steps.xsdvalidator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.vfs2.FileObject;
import org.apache.poi.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransTestFactory;

public class XsdValidatorIntTest {

  private static final String RAMDIR = "ram://" + XsdValidatorIntTest.class.getSimpleName();
  private static final String TEST_FILES_DIR = "src/test/resources/xsdvalidator/";

  private static FileObject schemaRamFile = null;
  private static FileObject dataRamFile = null;

  @BeforeClass
  public static void setUpBeforeClass() throws KettleException {
    KettleEnvironment.init( false );
  }

  @AfterClass
  public static void tearDownAfterClass() {
    try {
      if ( schemaRamFile != null && schemaRamFile.exists() ) {
        schemaRamFile.delete();
      }
      if ( dataRamFile != null && dataRamFile.exists() ) {
        dataRamFile.delete();
      }
    } catch ( Exception ignore ) {
      // Ignore
    }
  }

  @Test
  public void testVfsInputFiles() throws Exception {
    testVfsFileTypes( getDataRamFile().getURL().toString(), getSchemaRamFile().getURL().toString(), true );
    testVfsFileTypes( getDataRamFile().getURL().toString(), getSchemaFileUrl( TEST_FILES_DIR + "schema.xsd" ), true );
    testVfsFileTypes( getDataFileUrl( TEST_FILES_DIR + "data.xml" ), getSchemaRamFile().getURL().toString(), true );
    testVfsFileTypes( getDataFileUrl( TEST_FILES_DIR + "data.xml" ), getSchemaFileUrl( TEST_FILES_DIR + "schema.xsd" ), true );
    testVfsFileTypes( getDataFileUrl( TEST_FILES_DIR + "xsd_issue/bad.xml" ),
      getSchemaFileUrl( TEST_FILES_DIR + "xsd_issue/cbc-xml-schema-v1.0/CbcXML_v1.0.xsd" ), true );

  }

  private FileObject getSchemaRamFile() throws Exception {
    if ( schemaRamFile != null && schemaRamFile.exists() && schemaRamFile.getContent().getSize() > 0 ) {
      return schemaRamFile;
    }
    schemaRamFile = loadRamFile( "schema.xsd" );
    return schemaRamFile;
  }

  private FileObject getDataRamFile() throws Exception {
    if ( dataRamFile != null && dataRamFile.exists() && dataRamFile.getContent().getSize() > 0 ) {
      return dataRamFile;
    }
    dataRamFile = loadRamFile( "data.xml" );
    return dataRamFile;
  }

  private String getFileUrl( String filename ) throws Exception {
    File file = new File( filename );
    return file.toURI().toURL().toExternalForm();
  }

  private InputStream getFileInputStream( String filename ) throws Exception {
    File file = new File( TEST_FILES_DIR + filename );
    return new FileInputStream( file );
  }

  private String getSchemaFileUrl( String filename ) throws Exception {
    return getFileUrl( filename );
  }

  private String getDataFileUrl( String filename ) throws Exception {
    return getFileUrl( filename );
  }

  private FileObject loadRamFile( String filename ) throws Exception {
    String targetUrl = RAMDIR + "/" + filename;
    try ( InputStream source = getFileInputStream( filename ) ) {
      FileObject fileObject = KettleVFS.getInstance( DefaultBowl.getInstance() ).getFileObject( targetUrl );
      try ( OutputStream targetStream = fileObject.getContent().getOutputStream() ) {
        IOUtils.copy( source, targetStream );
      }
      return fileObject;
    }
  }

  private void testVfsFileTypes( String dataFilename, String schemaFilename, boolean expected ) throws Exception {
    assertNotNull( dataFilename );
    assertNotNull( schemaFilename );
    assertTrue( KettleVFS.getInstance( DefaultBowl.getInstance() ).getFileObject( dataFilename ).exists() );
    assertTrue( KettleVFS.getInstance( DefaultBowl.getInstance() ).getFileObject( schemaFilename ).exists() );

    RowMetaInterface inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta( new ValueMetaString( "DataFile" ) );
    inputRowMeta.addValueMeta( new ValueMetaString( "SchemaFile" ) );
    List<RowMetaAndData> inputData = new ArrayList<RowMetaAndData>();
    inputData.add( new RowMetaAndData( inputRowMeta, new Object[] { dataFilename, schemaFilename } ) );

    String stepName = "XSD Validator";
    XsdValidatorMeta meta = new XsdValidatorMeta();
    meta.setDefault();
    meta.setXMLSourceFile( true );
    meta.setXMLStream( "DataFile" );
    meta.setXSDSource( meta.SPECIFY_FIELDNAME );
    meta.setXSDDefinedField( "SchemaFile" );
    meta.setAddValidationMessage( true );
    TransMeta transMeta = TransTestFactory.generateTestTransformation( null, meta, stepName );

    List<RowMetaAndData> result = null;
    result =
        TransTestFactory.executeTestTransformation( transMeta, TransTestFactory.INJECTOR_STEPNAME, stepName,
            TransTestFactory.DUMMY_STEPNAME, inputData );

    assertNotNull( result );
    assertEquals( 1, result.size() );

    // Check Metadata
    assertEquals( ValueMetaInterface.TYPE_STRING, result.get( 0 ).getValueMeta( 0 ).getType() );
    assertEquals( ValueMetaInterface.TYPE_STRING, result.get( 0 ).getValueMeta( 1 ).getType() );
    assertEquals( ValueMetaInterface.TYPE_BOOLEAN, result.get( 0 ).getValueMeta( 2 ).getType() );
    assertEquals( "DataFile", result.get( 0 ).getValueMeta( 0 ).getName() );
    assertEquals( "SchemaFile", result.get( 0 ).getValueMeta( 1 ).getName() );
    assertEquals( "result", result.get( 0 ).getValueMeta( 2 ).getName() );

    // Check result
    assertEquals( dataFilename, result.get( 0 ).getString( 0, "default" ) );
    assertEquals( schemaFilename, result.get( 0 ).getString( 1, "default" ) );
    assertEquals( expected, result.get( 0 ).getBoolean( 2, !expected ) );
  }
}
