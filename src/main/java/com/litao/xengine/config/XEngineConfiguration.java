package com.litao.xengine.config;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tao Li on 9/16/14.
 */
public class XEngineConfiguration {
    private static String CONFIG_FILE = "xengine.properties";
    public static Configuration CONFIG = null;

    private static Logger LOG = LoggerFactory.getLogger(XEngineConfiguration.class);

    static {
        try {
            CONFIG = new PropertiesConfiguration(XEngineConfiguration.class.getClassLoader().getResource(CONFIG_FILE));
        } catch (ConfigurationException e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    private XEngineConfiguration() {

    }
}
