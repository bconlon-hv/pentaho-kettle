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


package org.pentaho.di.trans.steps.columnexists;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

/**
 * Check if a column exists in table on a specified connection *
 *
 * @author Samatar
 * @since 03-Juin-2008
 *
 */

public class ColumnExists extends BaseDatabaseStep implements StepInterface {
  private static Class<?> PKG = ColumnExistsMeta.class; // for i18n purposes, needed by Translator2!!

  private ColumnExistsMeta meta;
  private ColumnExistsData data;

  public ColumnExists( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (ColumnExistsMeta) smi;
    data = (ColumnExistsData) sdi;

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    // if no more input to be expected set done
    if ( r == null ) {
      setOutputDone();
      return false;
    }

    boolean columnexists = false;

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( getTransMeta().getBowl(), data.outputRowMeta, getStepname(), null, null, this, repository,
        metaStore );

      // Check is columnname field is provided
      if ( Utils.isEmpty( meta.getDynamicColumnnameField() ) ) {
        logError( BaseMessages.getString( PKG, "ColumnExists.Error.ColumnnameFieldMissing" ) );
        throw new KettleException( BaseMessages.getString( PKG, "ColumnExists.Error.ColumnnameFieldMissing" ) );
      }
      if ( meta.isTablenameInField() ) {
        // Check is tablename field is provided
        if ( Utils.isEmpty( meta.getDynamicTablenameField() ) ) {
          logError( BaseMessages.getString( PKG, "ColumnExists.Error.TablenameFieldMissing" ) );
          throw new KettleException( BaseMessages.getString( PKG, "ColumnExists.Error.TablenameFieldMissing" ) );
        }

        // cache the position of the field
        if ( data.indexOfTablename < 0 ) {
          data.indexOfTablename = getInputRowMeta().indexOfValue( meta.getDynamicTablenameField() );
          if ( data.indexOfTablename < 0 ) {
            // The field is unreachable !
            logError( BaseMessages.getString( PKG, "ColumnExists.Exception.CouldnotFindField" ) + "["
                + meta.getDynamicTablenameField() + "]" );
            throw new KettleException( BaseMessages.getString( PKG, "ColumnExists.Exception.CouldnotFindField",
              meta.getDynamicTablenameField() ) );
          }
        }
      } else {
        if ( !Utils.isEmpty( data.schemaname ) ) {
          data.tablename = data.db.getDatabaseMeta().getQuotedSchemaTableCombination( data.schemaname, data.tablename );
        } else {
          data.tablename = data.db.getDatabaseMeta().quoteField( data.tablename );
        }
      }

      // cache the position of the column field
      if ( data.indexOfColumnname < 0 ) {
        data.indexOfColumnname = getInputRowMeta().indexOfValue( meta.getDynamicColumnnameField() );
        if ( data.indexOfColumnname < 0 ) {
          // The field is unreachable !
          logError( BaseMessages.getString( PKG, "ColumnExists.Exception.CouldnotFindField" ) + "["
              + meta.getDynamicColumnnameField() + "]" );
          throw new KettleException( BaseMessages.getString( PKG, "ColumnExists.Exception.CouldnotFindField",
            meta.getDynamicColumnnameField() ) );
        }
      }

      // End If first
    }

    try {
      // get tablename
      if ( meta.isTablenameInField() ) {
        data.tablename = getInputRowMeta().getString( r, data.indexOfTablename );
        if ( !Utils.isEmpty( data.schemaname ) ) {
          data.tablename = data.db.getDatabaseMeta().getQuotedSchemaTableCombination( data.schemaname, data.tablename );
        } else {
          data.tablename = data.db.getDatabaseMeta().quoteField( data.tablename );
        }
      }
      // get columnname
      String columnname = getInputRowMeta().getString( r, data.indexOfColumnname );
      columnname = data.db.getDatabaseMeta().quoteField( columnname );

      // Check if table exists on the specified connection
      columnexists = data.db.checkColumnExists( columnname, data.tablename );

      Object[] outputRowData = RowDataUtil.addValueData( r, getInputRowMeta().size(), columnexists );

      // add new values to the row.
      putRow( data.outputRowMeta, outputRowData ); // copy row to output rowset(s);

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "ColumnExists.LineNumber",
          getLinesRead() + " : " + getInputRowMeta().getString( r ) ) );
      }
    } catch ( KettleException e ) {
      if ( getStepMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "ColumnExists.ErrorInStepRunning" + " : " + e.getMessage() ) );
        throw new KettleStepException( BaseMessages.getString( PKG, "ColumnExists.Log.ErrorInStep" ), e );
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, meta.getResultFieldName(), "ColumnExists001" );
      }
    }

    return true;
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ColumnExistsMeta) smi;
    data = (ColumnExistsData) sdi;

    if ( super.init( smi, sdi ) ) {
      if ( !meta.isTablenameInField() ) {
        if ( Utils.isEmpty( meta.getTablename() ) ) {
          logError( BaseMessages.getString( PKG, "ColumnExists.Error.TablenameMissing" ) );
          return false;
        }
        data.tablename = environmentSubstitute( meta.getTablename() );
      }
      data.schemaname = meta.getSchemaname();
      if ( !Utils.isEmpty( data.schemaname ) ) {
        data.schemaname = environmentSubstitute( data.schemaname );
      }

      if ( Utils.isEmpty( meta.getResultFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "ColumnExists.Error.ResultFieldMissing" ) );
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  protected Class<?> getPKG() {
    return PKG;
  }

  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ColumnExistsMeta) smi;
    data = (ColumnExistsData) sdi;
    super.dispose( smi, sdi );
  }
}
