//
// This file is part of the OpenNMS(R) Application.
//
// OpenNMS(R) is Copyright (C) 2006-2009 The OpenNMS Group, Inc. All rights
// reserved.
// OpenNMS(R) is a derivative work, containing both original code, included
// code
// and modified
// code that was published under the GNU General Public License. Copyrights
// for
// modified
// and included code are below.
//
// OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
//
// Modifications:
//
// 2009 Jul 23: Actually use URL parameters (bug 3266) - jeffg@opennms.org
// 2008 Dec 25: Make HttpCollectionSet have many HttpCollectionResources
// so that all resources get properly persisted when a collection
// has many URIs, without re-breaking storeByGroup for this
// collector (bug 2940)
// 2007 Aug 07: Move HTTP datacollection config package from
// org.opennms.netmgt.config.datacollection to
// org.opennms.netmgt.config.httpdatacollection. - dj@opennms.org
// 2003 Jan 31: Cleaned up some unused imports.
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

package org.opennms.netmgt.collectd;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.exolab.castor.xml.MarshalException;
import org.exolab.castor.xml.ValidationException;
import org.opennms.core.utils.ParameterMap;
import org.opennms.core.utils.ThreadCategory;
import org.opennms.core.utils.TimeKeeper;
import org.opennms.netmgt.collectd.HttpCollector.HttpCollectionSet;
import org.opennms.netmgt.config.DataSourceFactory;
import org.opennms.netmgt.config.PmacctCollectionConfigFactory;
import org.opennms.netmgt.config.collector.AttributeDefinition;
import org.opennms.netmgt.config.collector.AttributeGroup;
import org.opennms.netmgt.config.collector.AttributeGroupType;
import org.opennms.netmgt.config.collector.CollectionAttribute;
import org.opennms.netmgt.config.collector.CollectionAttributeType;
import org.opennms.netmgt.config.collector.CollectionResource;
import org.opennms.netmgt.config.collector.CollectionSet;
import org.opennms.netmgt.config.collector.CollectionSetVisitor;
import org.opennms.netmgt.config.collector.Persister;
import org.opennms.netmgt.config.collector.ServiceParameters;
import org.opennms.netmgt.config.pmacctdatacollection.Attrib;
import org.opennms.netmgt.config.pmacctdatacollection.PmacctCollection;
import org.opennms.netmgt.model.RrdRepository;
import org.opennms.netmgt.model.events.EventProxy;

/**
 * Collect data via PMAcct
 * 
 * @author <a href="mailto:david@opennms.org">David Hustace</a>
 * @author <a href="mailto:yann@atomes.com">Yann Vigara</a>
 * @version $Id: $
 */
public class PmacctCollector implements ServiceCollector {

    private NumberFormat parser = null;

    private NumberFormat rrdFormatter = null;

    /**
     * <p>
     * Constructor for PmacctCollector.
     * </p>
     */
    public PmacctCollector() {
        parser = NumberFormat.getNumberInstance();
        ((DecimalFormat) parser).setParseBigDecimal(true);

        rrdFormatter = NumberFormat.getNumberInstance();
        rrdFormatter.setMinimumFractionDigits(0);
        rrdFormatter.setMaximumFractionDigits(Integer.MAX_VALUE);
        rrdFormatter.setMinimumIntegerDigits(1);
        rrdFormatter.setMaximumIntegerDigits(Integer.MAX_VALUE);
        rrdFormatter.setGroupingUsed(false);

    }

    /** {@inheritDoc} */
    public CollectionSet collect(CollectionAgent agent, EventProxy eproxy, Map<String, Object> parameters) {
        PmacctCollectionSet collectionSet = new PmacctCollectionSet(agent, parameters);
        collectionSet.setCollectionTimestamp(new Date());
        collectionSet.collect();
        return collectionSet;
    }

    private ThreadCategory log() {
        return ThreadCategory.getInstance(getClass());
    }

    protected class PmacctCollectionSet implements CollectionSet {

        private CollectionAgent m_agent;

        private Map<String, Object> m_parameters;

        private int m_status;

        private PmacctCollection m_collection;

        private String mPmacctPath;

        private List<PmacctCollectionResource> m_collectionResourceList;
        
        private Date m_timestamp;

        PmacctCollectionSet(CollectionAgent agent,
                Map<String, Object> parameters) {
            m_agent = agent;
            m_parameters = parameters;
            m_status = ServiceCollector.COLLECTION_FAILED;
        }

        public void collect() {
            String collectionName = ParameterMap.getKeyedString(m_parameters,"collection",null);
            mPmacctPath = PmacctCollectionConfigFactory.getInstance().getPmacctPath();
            m_collection = PmacctCollectionConfigFactory.getInstance().getPmacctCollection(collectionName);
            m_collectionResourceList = new ArrayList<PmacctCollectionResource>();
            PmacctCollectionResource collectionResource = new PmacctCollectionResource(
                                                                                       m_agent,
                                                                                       m_collection.getName());
            try {
                doCollection(this, collectionResource);
                m_collectionResourceList.add(collectionResource);
            } catch (PmacctCollectorException e) {
                log().error("collect: pmacct collection failed: " + e, e);
                m_status = ServiceCollector.COLLECTION_FAILED;
                return;
            }
            m_status = ServiceCollector.COLLECTION_SUCCEEDED;
        }

        public CollectionAgent getAgent() {
            return m_agent;
        }

        public void setAgent(CollectionAgent agent) {
            m_agent = agent;
        }

        public Map<String, Object> getParameters() {
            return m_parameters;
        }

        public void setParameters(Map<String, Object> parameters) {
            m_parameters = parameters;
        }

        public int getStatus() {
            return m_status;
        }

        public void storeResults(List<PmacctCollectionAttribute> results,
                PmacctCollectionResource collectionResource) {
            collectionResource.storeResults(results);
        }

        public void visit(CollectionSetVisitor visitor) {
            visitor.visitCollectionSet(this);
            for (PmacctCollectionResource collectionResource : m_collectionResourceList) {
                collectionResource.visit(visitor);
            }
            visitor.completeCollectionSet(this);
        }

        public boolean ignorePersist() {
            return false;
        }

        /**
         * @return the m_collection
         */
        public PmacctCollection getCollection() {
            return m_collection;
        }

        /**
         * @param pM_collection
         *            the m_collection to set
         */
        public void setCollection(PmacctCollection pCollection) {
            m_collection = pCollection;
        }

        /**
         * @return the pmacctPath
         */
        public String getPmacctPath() {
            return mPmacctPath;
        }

        /**
         * @param pPmacctPath
         *            the pmacctPath to set
         */
        public void setPmacctPath(String pPmacctPath) {
            mPmacctPath = pPmacctPath;
        }

        @Override
        public Date getCollectionTimestamp() {
                return m_timestamp;
        }
        public void setCollectionTimestamp(Date timestamp) {
                this.m_timestamp = timestamp;
        }
    }

    /**
     * Performs HTTP collection. Couple of notes to make the implementation of
     * this client library less obtuse: - HostConfiguration class is not
     * created here because the library builds it when a URI is defined.
     * 
     * @param collectionSet
     * @throws PmacctCollectorException
     */
    private void doCollection(final PmacctCollectionSet collectionSet,
            final PmacctCollectionResource collectionResource)
            throws PmacctCollectorException {
        String pmacctCmd = "";
        try {
            persistResponse(collectionSet, collectionResource, pmacctCmd);
        } catch (IOException e) {
            throw new PmacctCollectorException("IO Error retrieving page",
                                               pmacctCmd);
        }
    }

    class PmacctCollectionAttribute extends AbstractCollectionAttribute
            implements AttributeDefinition {

        String m_alias;

        String m_type;

        Object m_value;

        PmacctCollectionResource m_resource;

        PmacctCollectionAttributeType m_attribType;

        PmacctCollectionAttribute(PmacctCollectionResource resource,
                PmacctCollectionAttributeType attribType, String alias,
                String type, Number value) {
            super();
            m_resource = resource;
            m_attribType = attribType;
            m_alias = alias;
            m_type = type;
            m_value = value;
        }

        PmacctCollectionAttribute(PmacctCollectionResource resource,
                PmacctCollectionAttributeType attribType, String alias,
                String type, String value) {
            super();
            m_resource = resource;
            m_attribType = attribType;
            m_alias = alias;
            m_type = type;
            m_value = value;
        }

        @Override
        public String getName() {
            return m_alias;
        }

        public String getType() {
            return m_type;
        }

        public Object getValue() {
            return m_value;
        }

        @Override
        public String getNumericValue() {
            Object val = getValue();
            if (val instanceof Number) {
                return val.toString();
            } else {
                try {
                    return Double.valueOf(val.toString()).toString();
                } catch (NumberFormatException nfe) { /* Fall through */
                }
            }
            if (log().isDebugEnabled()) {
                log().debug("Value for attribute "
                                    + this.toString()
                                    + " does not appear to be a number, skipping");
            }
            return null;
        }

        @Override
        public String getStringValue() {
            return getValue().toString();
        }

        public String getValueAsString() {
            if (m_value instanceof Number) {
                return rrdFormatter.format(m_value);
            } else {
                return m_value.toString();
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof PmacctCollectionAttribute) {
                PmacctCollectionAttribute other = (PmacctCollectionAttribute) obj;
                return getName().equals(other.getName());
            }
            return false;
        }

        @Override
        public CollectionAttributeType getAttributeType() {
            return m_attribType;
        }

        @Override
        public CollectionResource getResource() {
            return m_resource;
        }

        @Override
        public boolean shouldPersist(ServiceParameters params) {
            return true;
        }

        @Override
        public int hashCode() {
            return getName().hashCode();
        }

        @Override
        public String toString() {
            StringBuffer buffer = new StringBuffer();
            buffer.append("PmacctAttribute: ");
            buffer.append(getName());
            buffer.append(":");
            buffer.append(getType());
            buffer.append(":");
            buffer.append(getValueAsString());
            return buffer.toString();
        }

    }

    private List<PmacctCollectionAttribute> processResponse(
            final PmacctCollectionSet collectionSet,
            PmacctCollectionResource collectionResource) {
        log().debug("processResponse:");
        List<PmacctCollectionAttribute> butes = new LinkedList<PmacctCollectionAttribute>();

        PmacctCollection lCollection = collectionSet.getCollection();
        List<Attrib> attribDefs = lCollection.getAttributes().getAttribCollection();
        AttributeGroupType groupType = new AttributeGroupType(
                                                              lCollection.getName(),
                                                              "all");

        String lPmacctPath = collectionSet.getPmacctPath();
        String lAddress = collectionResource.m_ipAddress;

        for (Attrib attribDef : attribDefs) {
            String lCmd = lPmacctPath + " "
                    + attribDef.getPmacctOptions().replaceAll("%h", lAddress);
            try {
                Process child = Runtime.getRuntime().exec(lCmd);

                // Get the input stream and read from it
                InputStream stdout = child.getInputStream();
                BufferedReader brCleanUp = new BufferedReader(
                                                              new InputStreamReader(
                                                                                    stdout));
                String line;
                while ((line = brCleanUp.readLine()) != null) {
                    System.out.println("[Stdout] " + line);

                    if (!attribDef.getType().matches("^([Oo](ctet|CTET)[Ss](tring|TRING))|([Ss](tring|TRING))$")) {
                        Number num = NumberFormat.getNumberInstance().parse(line);
                        // m.group(attribDef.getMatchGroup()));
                        PmacctCollectionAttribute bute = new PmacctCollectionAttribute(
                                                                                       collectionResource,
                                                                                       new PmacctCollectionAttributeType(
                                                                                                                         attribDef,
                                                                                                                         groupType),
                                                                                       attribDef.getAlias(),
                                                                                       attribDef.getType(),
                                                                                       num);
                        log().debug("processResponse: adding found numeric attribute: "
                                            + bute);
                        butes.add(bute);
                    } else {
                        PmacctCollectionAttribute bute = new PmacctCollectionAttribute(
                                                                                       collectionResource,
                                                                                       new PmacctCollectionAttributeType(
                                                                                                                         attribDef,
                                                                                                                         groupType),
                                                                                       attribDef.getAlias(),
                                                                                       attribDef.getType(),
                                                                                       line);
                        log().debug("processResponse: adding found string attribute: "
                                            + bute);
                        butes.add(bute);
                    }
                }
                brCleanUp.close();

            } catch (Exception e) {
                System.out.println("erreur d'execution " + lCmd
                        + e.toString());
            }

        }
        return butes;
    }

    public class PmacctCollectorException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        String m_cmd;

        PmacctCollectorException(String message, String pCommand) {
            super(message);
            m_cmd = pCommand;
        }

        @Override
        public String toString() {
            return super.toString() + ": command : " + m_cmd;
        }
    }

    private void persistResponse(final PmacctCollectionSet collectionSet,
            PmacctCollectionResource collectionResource, String pmacctCmd)
            throws IOException {

        List<PmacctCollectionAttribute> butes = processResponse(collectionSet,
                                                                collectionResource);

        if (butes.isEmpty()) {
            throw new PmacctCollectorException(
                                               "No attributes specified were found: ",
                                               pmacctCmd);
        }

        // put the results into the collectionset for later
        collectionSet.storeResults(butes, collectionResource);
    }

    /** {@inheritDoc} */
    public void initialize(Map<String, String> parameters) {

        log().debug("initialize: Initializing PmacctCollector.");

        initPmacctCollecionConfig();
        initDatabaseConnectionFactory();
        initializeRrdRepository();
    }

    private void initPmacctCollecionConfig() {
        try {
            log().debug("initialize: Initializing collector: " + getClass());
            PmacctCollectionConfigFactory.init();
        } catch (MarshalException e) {
            log().fatal("initialize: Error marshalling configuration.", e);
            throw new UndeclaredThrowableException(e);
        } catch (ValidationException e) {
            log().fatal("initialize: Error validating configuration.", e);
            throw new UndeclaredThrowableException(e);
        } catch (FileNotFoundException e) {
            log().fatal("initialize: Error locating configuration.", e);
            throw new UndeclaredThrowableException(e);
        } catch (IOException e) {
            log().fatal("initialize: Error reading configuration", e);
            throw new UndeclaredThrowableException(e);
        }
    }

    private void initializeRrdRepository() {
        log().debug("initializeRrdRepository: Initializing RRD repo from PmacctCollector...");
        initializeRrdDirs();
    }

    private void initializeRrdDirs() {
        /*
         * If the RRD file repository directory does NOT already exist, create
         * it.
         */
        StringBuffer sb;
        File f = new File(
                          PmacctCollectionConfigFactory.getInstance().getRrdPath());
        if (!f.isDirectory()) {
            if (!f.mkdirs()) {
                sb = new StringBuffer();
                sb.append("initializeRrdDirs: Unable to create RRD file repository.  Path doesn't already exist and could not make directory: ");
                sb.append(PmacctCollectionConfigFactory.getInstance().getRrdPath());
                log().error(sb.toString());
                throw new RuntimeException(sb.toString());
            }
        }
    }

    private void initDatabaseConnectionFactory() {
        try {
            DataSourceFactory.init();
        } catch (IOException e) {
            log().fatal("initDatabaseConnectionFactory: IOException getting database connection",
                        e);
            throw new UndeclaredThrowableException(e);
        } catch (MarshalException e) {
            log().fatal("initDatabaseConnectionFactory: Marshall Exception getting database connection",
                        e);
            throw new UndeclaredThrowableException(e);
        } catch (ValidationException e) {
            log().fatal("initDatabaseConnectionFactory: Validation Exception getting database connection",
                        e);
            throw new UndeclaredThrowableException(e);
        } catch (SQLException e) {
            log().fatal("initDatabaseConnectionFactory: Failed getting connection to the database.",
                        e);
            throw new UndeclaredThrowableException(e);
        } catch (PropertyVetoException e) {
            log().fatal("initDatabaseConnectionFactory: Failed getting connection to the database.",
                        e);
            throw new UndeclaredThrowableException(e);
        } catch (ClassNotFoundException e) {
            log().fatal("initDatabaseConnectionFactory: Failed loading database driver.",
                        e);
            throw new UndeclaredThrowableException(e);
        }
    }

    /** {@inheritDoc} */
    public void initialize(CollectionAgent agent,
            Map<String, Object> parameters) {
        InetAddress ipAddr = (InetAddress) agent.getAddress();
        if (log().isDebugEnabled()) {
            log().debug("initialize: InetAddress=" + ipAddr.getHostAddress());
        }
    }

    /**
     * <p>
     * release
     * </p>
     */
    public void release() {
        // TODO Auto-generated method stub
    }

    /** {@inheritDoc} */
    public void release(CollectionAgent agent) {
        // TODO Auto-generated method stub
    }

    class PmacctCollectionResource implements CollectionResource {

        int m_nodeId;

        String m_ipAddress;

        String m_resourceName;

        AttributeGroup m_attribGroup;

        PmacctCollectionResource(CollectionAgent agent, String resourceName) {
            m_ipAddress = agent.getHostAddress();
            m_nodeId = agent.getNodeId();
            m_resourceName = resourceName;
            m_attribGroup = new AttributeGroup(
                                               this,
                                               new AttributeGroupType(
                                                                      resourceName,
                                                                      "all"));

        }

        public void storeResults(List<PmacctCollectionAttribute> results) {
            for (PmacctCollectionAttribute attrib : results) {
                m_attribGroup.addAttribute(attrib);
            }
        }

        // A rescan is never needed for the PmacctCollector
        public boolean rescanNeeded() {
            return false;
        }

        public boolean shouldPersist(ServiceParameters params) {
            return true;
        }

        public String getOwnerName() {
            return m_ipAddress;
        }

        public File getResourceDir(RrdRepository repository) {
            return new File(repository.getRrdBaseDir(), m_nodeId
                    + File.separator + m_ipAddress);
        }

        public void visit(CollectionSetVisitor visitor) {
            visitor.visitResource(this);
            m_attribGroup.visit(visitor);
            visitor.completeResource(this);
        }

        public int getType() {
            return 0;
        }

        public String getResourceTypeName() {
            return "if";
        }

        public String getInstance() {
            return m_ipAddress + "[" + m_resourceName + "]";
        }

        public String getLabel() {
            return null;
        }

        @Override
        public String toString() {
            return m_resourceName + "@" + m_ipAddress;
        }

        @Override
        public String getParent() {
            return m_ipAddress;
        }

        @Override
        public TimeKeeper getTimeKeeper() {
            return null;
        }
    }

    class PmacctCollectionAttributeType implements CollectionAttributeType {

        Attrib m_attribute;

        AttributeGroupType m_groupType;

        protected PmacctCollectionAttributeType(Attrib attribute,
                AttributeGroupType groupType) {
            m_groupType = groupType;
            m_attribute = attribute;
        }

        public AttributeGroupType getGroupType() {
            return m_groupType;
        }

        public void storeAttribute(CollectionAttribute attribute,
                Persister persister) {
            persister.persistNumericAttribute(attribute);
        }

        public String getName() {
            return m_attribute.getAlias();
        }

        public String getType() {
            return m_attribute.getType();
        }

    }

    /** {@inheritDoc} */
    public RrdRepository getRrdRepository(String collectionName) {
        return PmacctCollectionConfigFactory.getInstance().getRrdRepository(collectionName);
    }

}
