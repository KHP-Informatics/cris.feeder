
/*
 * $Id: JSONUtils.java,v 1.1 2012/02/01 00:00:46 hhwu Exp $
 */

package kcl.iop.brc.core.kconnect.utils;

import java.io.StringWriter;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
 * Some basic utilities for processing JSON.  This class depends on Jackson.
 *
 * @author Jeff Schnitzer
 */
public class JSONUtils
{
        /**
         * Converts the object to a JSON string
         */
        public static String toJSON(Object value)
        {
                ObjectMapper mapper = new ObjectMapper();
                try
                {
                        StringWriter writer = new StringWriter();

                        mapper.writeValue(writer, value);

                        return writer.toString();

                        // This requires Jackson 1.3 which breaks GAE.
                        // TODO, replace with this code when bug is fixed:
                        // http://jira.codehaus.org/browse/JACKSON-188
                        //return mapper.writeValueAsString(value);
                }
                catch (RuntimeException ex) { throw ex; }
                catch (Exception ex) { throw new RuntimeException(ex); }
        }

        /**
         * Converts the JSON string to an Object (either Map or List)
         */
        public static Object fromJSON(String value)
        {
                return fromJSON(value, Object.class);
        }

        /**
         * Converts the JSON string to a typed object via a TypeReference
         * The main complication is handling of Generic types: if they are used, one
         * has to use TypeReference object, to work around Java Type Erasure.
         *
         * ex: return JSONUtils.fromJSON(this.answersJson, new TypeReference<List<StanzaAnswer>>(){});
         */
        public static <T> T fromJSON(String value, TypeReference<T> type)
        {
                ObjectMapper mapper = new ObjectMapper();
                try
                {
                        return mapper.readValue(value, type);
                }
                catch (RuntimeException ex) { throw ex; }
                catch (Exception ex) { throw new RuntimeException(ex); }
        }

        /**
         * Converts the JSON string to a typed object (or Map/List if Object.class is passed in)
         */
        public static <T> T fromJSON(String value, Class<T> type)
        {
                ObjectMapper mapper = new ObjectMapper();
                try
                {
                        return mapper.readValue(value, type);
                }
                catch (RuntimeException ex) { throw ex; }
                catch (Exception ex) { throw new RuntimeException(ex); }
        }
}