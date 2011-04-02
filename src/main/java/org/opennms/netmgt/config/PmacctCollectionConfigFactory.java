//
// This file is part of the OpenNMS(R) Application.
//
// OpenNMS(R) is Copyright (C) 2006 The OpenNMS Group, Inc. All rights reserved.
// OpenNMS(R) is a derivative work, containing both original code, included code
// and modified
// code that was published under the GNU General Public License. Copyrights for
// modified
// and included code are below.
//
// OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
//
// Modifications:
//
// 2007 Aug 07: Move HTTP datacollection config package from
// org.opennms.netmgt.config.datacollection to
// org.opennms.netmgt.config.httpdatacollection. - dj@opennms.org
// 2003 Jan 31: Cleaned up some unused imports.
//
// Original code base Copyright (C) 1999-2001 Oculan Corp. All rights reserved.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
//
// For more information contact:
// OpenNMS Licensing <license@opennms.org>
// http://www.opennms.org/
// http://www.opennms.com/
//
//

package org.opennms.netmgt.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.exolab.castor.xml.MarshalException;
import org.exolab.castor.xml.ValidationException;
import org.opennms.core.utils.ThreadCategory;
import org.opennms.netmgt.config.pmacctdatacollection.PmacctCollection;
import org.opennms.netmgt.config.pmacctdatacollection.PmacctDatacollectionConfig;
import org.opennms.netmgt.dao.castor.CastorUtils;
import org.opennms.netmgt.model.RrdRepository;

/**
 * <p>
 * PmacctCollectionConfigFactory class.
 * </p>
 * 
 * @author <a href="mailto:david@opennms.org">David Hustace</a>
 * @author <a href="mailto:yann@atomes.com">Yann Vigara</a>
 * @version $Id: $
 */
public class PmacctCollectionConfigFactory
{

    /** The singleton instance. */
    private static PmacctCollectionConfigFactory m_instance;

    private static boolean m_loadedFromFile = false;

    /** Boolean indicating if the init() method has been called. */
    protected boolean initialized = false;

    /**
     * Timestamp of the http collection config, used to know when to reload from
     * disk.
     */
    protected static long m_lastModified;

    private static PmacctDatacollectionConfig m_config;

    /**
     * <p>
     * Constructor for PmacctCollectionConfigFactory.
     * </p>
     * 
     * @param configFile
     *            a {@link java.lang.String} object.
     * @throws org.exolab.castor.xml.MarshalException
     *             if any.
     * @throws org.exolab.castor.xml.ValidationException
     *             if any.
     * @throws java.io.IOException
     *             if any.
     */
    public PmacctCollectionConfigFactory(String configFile)
    throws MarshalException, ValidationException, IOException
    {
        InputStream is = null;
        try
        {
            is = new FileInputStream(configFile);
            initialize(is);
        }
        finally
        {
            if (is != null)
            {
                IOUtils.closeQuietly(is);
            }
        }
    }

    /**
     * <p>
     * Constructor for HttpCollectionConfigFactory.
     * </p>
     * 
     * @param stream
     *            a {@link java.io.InputStream} object.
     * @throws org.exolab.castor.xml.MarshalException
     *             if any.
     * @throws org.exolab.castor.xml.ValidationException
     *             if any.
     */
    public PmacctCollectionConfigFactory(InputStream stream)
    throws MarshalException, ValidationException
    {
        initialize(stream);
    }

    private void initialize(InputStream stream)
    throws MarshalException, ValidationException
    {
        log().debug("initialize: initializing http collection config factory.");
        m_config = CastorUtils.unmarshal(PmacctDatacollectionConfig.class, stream, false);
    }

    /**
     * Be sure to call this method before calling getInstance().
     * 
     * @throws java.io.IOException
     *             if any.
     * @throws java.io.FileNotFoundException
     *             if any.
     * @throws org.exolab.castor.xml.MarshalException
     *             if any.
     * @throws org.exolab.castor.xml.ValidationException
     *             if any.
     */
    public static synchronized void init()
    throws IOException, FileNotFoundException, MarshalException, ValidationException
    {

        if (m_instance == null)
        {
            File cfgFile = PmacctConfigFileConstants
                .getFile(PmacctConfigFileConstants.PMACCT_COLLECTION_CONFIG_FILE_NAME);
            m_instance = new PmacctCollectionConfigFactory(cfgFile.getPath());
            m_lastModified = cfgFile.lastModified();
            m_loadedFromFile = true;
        }
    }

    /**
     * Singleton static call to get the only instance that should exist
     * 
     * @return the single factory instance
     * @throws java.lang.IllegalStateException
     *             if init has not been called
     */
    public static synchronized PmacctCollectionConfigFactory getInstance()
    {

        if (m_instance == null)
        {
            throw new IllegalStateException(
                "You must call PmacctCollectionConfigFactory.init() before calling getInstance().");
        }
        return m_instance;
    }

    /**
     * <p>
     * setInstance
     * </p>
     * 
     * @param instance
     *            a
     *            {@link org.opennms.netmgt.config.PmacctCollectionConfigFactory}
     *            object.
     */
    public static synchronized void setInstance(PmacctCollectionConfigFactory instance)
    {
        m_instance = instance;
        m_loadedFromFile = false;
    }

    /**
     * <p>
     * reload
     * </p>
     * 
     * @throws java.io.IOException
     *             if any.
     * @throws java.io.FileNotFoundException
     *             if any.
     * @throws org.exolab.castor.xml.MarshalException
     *             if any.
     * @throws org.exolab.castor.xml.ValidationException
     *             if any.
     */
    public synchronized void reload()
    throws IOException, FileNotFoundException, MarshalException, ValidationException
    {
        m_instance = null;
        init();
    }

    /**
     * Reload the http-datacollection-config.xml file if it has been changed
     * since we last
     * read it.
     * 
     * @throws java.io.IOException
     *             if any.
     * @throws org.exolab.castor.xml.MarshalException
     *             if any.
     * @throws org.exolab.castor.xml.ValidationException
     *             if any.
     */
    protected void updateFromFile()
    throws IOException, MarshalException, ValidationException
    {
        if (m_loadedFromFile)
        {
            File surveillanceViewsFile = PmacctConfigFileConstants
                .getFile(PmacctConfigFileConstants.PMACCT_COLLECTION_CONFIG_FILE_NAME);
            if (m_lastModified != surveillanceViewsFile.lastModified())
            {
                this.reload();
            }
        }
    }

    /**
     * <p>
     * getConfig
     * </p>
     * 
     * @return a
     *         {@link org.opennms.netmgt.config.httpdatacollection.HttpDatacollectionConfig}
     *         object.
     */
    public synchronized static PmacctDatacollectionConfig getConfig()
    {
        return m_config;
    }

    /**
     * <p>
     * setConfig
     * </p>
     * 
     * @param m_config
     *            a
     *            {@link org.opennms.netmgt.config.httpdatacollection.HttpDatacollectionConfig}
     *            object.
     */
    public synchronized static void setConfig(PmacctDatacollectionConfig m_config)
    {
        PmacctCollectionConfigFactory.m_config = m_config;
    }

    private ThreadCategory log()
    {
        return ThreadCategory.getInstance();
    }

    /**
     * <p>
     * getHttpCollection
     * </p>
     * 
     * @param collectionName
     *            a {@link java.lang.String} object.
     * @return a
     *         {@link org.opennms.netmgt.config.httpdatacollection.HttpCollection}
     *         object.
     */
    public PmacctCollection getPmacctCollection(String collectionName)
    {
        List<PmacctCollection> collections = m_config.getPmacctCollectionCollection();
        PmacctCollection collection = null;
        for (PmacctCollection coll : collections)
        {
            if (coll.getName().equalsIgnoreCase(collectionName))
            {
                collection = coll;
                break;
            }
        }
        if (collection == null)
        {
            throw new IllegalArgumentException(
                "getPmacctCollection: collection name: "
                    + collectionName
                    + " specified in collectd configuration not found in http collection configuration.");
        }
        return collection;
    }

    /**
     * <p>
     * getRrdRepository
     * </p>
     * 
     * @param collectionName
     *            a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.model.RrdRepository} object.
     */
    public RrdRepository getRrdRepository(String collectionName)
    {
        RrdRepository repo = new RrdRepository();
        repo.setRrdBaseDir(new File(getRrdPath()));
        repo.setRraList(getRRAList(collectionName));
        repo.setStep(getStep(collectionName));
        repo.setHeartBeat((2 * getStep(collectionName)));
        return repo;
    }

    /**
     * <p>
     * getStep
     * </p>
     * 
     * @param cName
     *            a {@link java.lang.String} object.
     * @return a int.
     */
    public int getStep(String cName)
    {
        PmacctCollection collection = getPmacctCollection(cName);
        if (collection != null)
        {
            return collection.getRrd().getStep();
        }
        else
        {
            return -1;
        }
    }

    /**
     * <p>
     * getRRAList
     * </p>
     * 
     * @param cName
     *            a {@link java.lang.String} object.
     * @return a {@link java.util.List} object.
     */
    public List<String> getRRAList(String cName)
    {
        PmacctCollection collection = getPmacctCollection(cName);
        if (collection != null)
        {
            return collection.getRrd().getRraCollection();
        }
        else
        {
            return null;
        }

    }

    /**
     * <p>
     * getRrdPath
     * </p>
     * 
     * @return a {@link java.lang.String} object.
     */
    public String getRrdPath()
    {
        String rrdPath = m_config.getRrdRepository();
        if (rrdPath == null)
        {
            throw new RuntimeException("Configuration error, failed to "
                + "retrieve path to RRD repository.");
        }

        /*
         * TODO: make a path utils class that has the below in it strip the
         * File.separator char off of the end of the path.
         */
        if (rrdPath.endsWith(File.separator))
        {
            rrdPath = rrdPath.substring(0, (rrdPath.length() - File.separator.length()));
        }

        return rrdPath;
    }

    public String getPmacctPath()
    {
        String pmacctPath = m_config.getPmacctPath();
        if (pmacctPath == null)
        {
            throw new RuntimeException("Configuration error, failed to "
                + "retrieve path to Pmacct command.");
        }
        File pmacct = new File(pmacctPath);
        if (!pmacct.exists())
        {
            throw new RuntimeException("Configuration error, failed to "
                + "find the Pmacct executable at : " + pmacctPath);
        }
        return pmacctPath;
    }

}
