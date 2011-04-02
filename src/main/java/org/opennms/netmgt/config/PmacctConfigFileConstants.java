//
// This file is part of the OpenNMS(R) Application.
//
// OpenNMS(R) is Copyright (C) 2002-2008 The OpenNMS Group, Inc. All rights
// reserved.
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
// 2008 May 12: Add xmp-config and xmp-datacollection-config. -
// jeffg@opennms.org
// 2006 Sep 10: Better error reporting, some code formatting. - dj@opennms.org
// 2003 Nov 11: Merged changes from Rackspace project
// 2003 Sep 03: Minor opennms-server changes
// 2003 Aug 29: Added a server-config file
// 2003 Aug 21: Added ScriptD related files
// 2003 Feb 04: Added Key SNMP Custom Reports
// 2002 Nov 10: Added a new XML file: webui-colors.xml
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
package org.opennms.netmgt.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.opennms.core.utils.LogUtils;
import org.opennms.netmgt.ConfigFileConstants;

/**
 * @author <a href="mailto:yann@atomes.com">Yann Vigara</a>
 */
public class PmacctConfigFileConstants
{

    public static final int PMACCT_COLLECTION_CONFIG_FILE_NAME;

    private static final String[] FILE_ID_TO_NAME;

    static
    {
        PMACCT_COLLECTION_CONFIG_FILE_NAME = 81;
        FILE_ID_TO_NAME = new String[82];
        FILE_ID_TO_NAME[PMACCT_COLLECTION_CONFIG_FILE_NAME] = "pmacct-datacollection-config.xml";
    }

    public static final File getFile(int id)
    throws IOException
    {
        // Recover the home directory from the system properties.
        String home = ConfigFileConstants.getHome();

        // Check to make sure that the home directory exists
        File fhome = new File(home);
        if (!fhome.exists())
        {
            LogUtils.debugf(ConfigFileConstants.class,
                "The specified home directory does not exist.");
            throw new FileNotFoundException("The OpenNMS home directory \"" + home
                + "\" does not exist");
        }

        String rfile = getFileName(id);
        File frfile = new File(home + File.separator + "etc" + File.separator + rfile);
        if (!frfile.exists())
        {
            File frfileNoEtc = new File(home + File.separator + rfile);
            if (!frfileNoEtc.exists())
            {
                throw new FileNotFoundException("The requested file '" + rfile
                    + "' could not be found at '" + frfile.getAbsolutePath() + "' or '"
                    + frfileNoEtc.getAbsolutePath() + "'");
            }
        }

        return frfile;
    }

    public static final String getFileName(int id)
    {
        return FILE_ID_TO_NAME[id];
    }
}
