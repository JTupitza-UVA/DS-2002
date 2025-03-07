/*=====================================================================
File: 	 ConstrainedDelegation.java
Summary: This Microsoft JDBC Driver for SQL Server sample application 
         that demonstrates how to establish constrained delegation connection.
         An intermediate service is necessary to impersonate the client. 
		 This service needs to be configured with the options:
		 1. "Trust this user for delegation to specified services only"
		 2. "Use any authentication protocol"
---------------------------------------------------------------------
This file is part of the Microsoft JDBC Driver for SQL Server Code Samples.
Copyright (C) Microsoft Corporation.  All rights reserved.
 
This source code is intended only as a supplement to Microsoft
Development Tools and/or on-line documentation.  See these other
materials for detailed information regarding Microsoft code samples.
 
THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF
ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
PARTICULAR PURPOSE.
=====================================================================*/

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import com.sun.security.auth.module.Krb5LoginModule;
import com.sun.security.jgss.ExtendedGSSCredential;


public class ConstrainedDelegation {

    // Connection properties
    private static final String CONNECTION_URI = "jdbc:sqlserver://<server>:<port>";

    private static final String TARGET_USER_NAME = "User to be impersonated";

    // Impersonation service properties
    private static final String SERVICE_PRINCIPAL = "SPN";
    private static final String KEYTAB_ROUTE = "<Route to Keytab file>";

    private static final Properties driverProperties;
    private static Oid krb5Oid;
    private static final String KERBEROS_OID = "1.2.840.113554.1.2.2";

    private static Subject serviceSubject;

    static {

        driverProperties = new Properties();
        driverProperties.setProperty("integratedSecurity", "true");
        driverProperties.setProperty("authenticationScheme", "JavaKerberos");

        try {
            krb5Oid = new Oid(KERBEROS_OID);
        } catch (GSSException e) {
            System.out.println("Error creating Oid: " + e);
            System.exit(-1);
        }
    }

    public static void main(String... args) throws Exception {
        System.out.println("Service subject: " + doInitialLogin());

        // Get impersonated user credentials thanks S4U2self mechanism
        GSSCredential impersonatedUserCreds = impersonate();
        System.out.println("Credentials for " + TARGET_USER_NAME + ": " + impersonatedUserCreds);

        // Create a connection for target service thanks S4U2proxy mechanism
        try (Connection con = createConnection(impersonatedUserCreds)) {
            System.out.println("Connection succesfully: " + con);
        }
    }

    /**
     *
     * Authenticate the intermediate server that is going to impersonate the client
     *
     * @return a subject for the intermediate server with the keytab credentials
     * @throws PrivilegedActionException
     *         in case of failure
     */
    private static Subject doInitialLogin() throws PrivilegedActionException {
        serviceSubject = new Subject();

        LoginModule krb5Module;
        try {
            krb5Module = (LoginModule) new Krb5LoginModule();
        } catch (Exception e) {
            System.out.print("Error loading Krb5LoginModule module: " + e);
            throw new PrivilegedActionException(e);
        }

        System.setProperty("sun.security.krb5.debug", String.valueOf(true));

        Map<String, String> options = new HashMap<>();
        options.put("useKeyTab", "true");
        options.put("storeKey", "true");
        options.put("doNotPrompt", "true");
        options.put("keyTab", KEYTAB_ROUTE);
        options.put("principal", SERVICE_PRINCIPAL);
        options.put("debug", "true");
        options.put("isInitiator", "true");

        Map<String, String> sharedState = new HashMap<>(0);

        krb5Module.initialize(serviceSubject, null, sharedState, options);
        try {
            krb5Module.login();
            krb5Module.commit();
            krb5Module.logout();
        } catch (LoginException e) {
            System.out.print("Error authenticating with Kerberos: " + e);
            try {
                krb5Module.abort();
            } catch (LoginException e1) {
                System.out.print("Error aborting Kerberos authentication:  " + e1);
                throw new PrivilegedActionException(e);
            }
            throw new PrivilegedActionException(e);
        }
        return serviceSubject;
    }

    /**
     * Generate the impersonated user credentials thanks to the S4U2self mechanism
     *
     * @return the client impersonated GSSCredential
     * @throws PrivilegedActionException
     *         in case of failure
     */
    private static GSSCredential impersonate() throws PrivilegedActionException {
        return Subject.doAs(serviceSubject, (PrivilegedExceptionAction<GSSCredential>) () -> {
            GSSManager manager = GSSManager.getInstance();

            GSSCredential self = manager.createCredential(null, GSSCredential.DEFAULT_LIFETIME, krb5Oid,
                    GSSCredential.INITIATE_ONLY);
            GSSName user = manager.createName(TARGET_USER_NAME, GSSName.NT_USER_NAME);
            return ((ExtendedGSSCredential) self).impersonate(user);
        });
    }

    /**
     * Obtains a connection using an impersonated credential
     *
     * @param impersonatedUserCredential
     *        impersonated user credentials
     * @return a connection to the SQL Server opened using the given impersonated credential
     * @throws PrivilegedActionException
     *         in case of failure
     */
    private static Connection createConnection(
            final GSSCredential impersonatedUserCredential) throws PrivilegedActionException {

        return Subject.doAs(new Subject(), (PrivilegedExceptionAction<Connection>) () -> {
            driverProperties.put("gsscredential", impersonatedUserCredential);
            return DriverManager.getConnection(CONNECTION_URI, driverProperties);
        });
    }

}
