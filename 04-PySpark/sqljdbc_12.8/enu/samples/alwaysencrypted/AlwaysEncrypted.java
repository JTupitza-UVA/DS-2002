/*=====================================================================
File: 	 AlwaysEncrypted.java
Summary: This Microsoft JDBC Driver for SQL Server sample application
         demonstrates how to create Column Master Key and Column Encryption Key for use 
		 with the Java Key Store for Always Encrypted feature.

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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionJavaKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerException;


public class AlwaysEncrypted {
    // Alias of the key stored in the keystore.
    private static String keyAlias = null;

    // Name by which the column master key will be known in the database.
    private static String columnMasterKeyName = "JDBC_CMK";

    // Name by which the column encryption key will be known in the database.
    private static String columnEncryptionKey = "JDBC_CEK";

    // The location of the keystore.
    private static String keyStoreLocation = null;

    // The password of the keystore and the key.
    private static char[] keyStoreSecret = null;

    /**
     * Name of the encryption algorithm used to encrypt the value of the column encryption key. The algorithm for the
     * system providers must be RSA_OAEP.
     */
    private static String algorithm = "RSA_OAEP";

    public static void main(String[] args) {

        String serverName = null;
        String portNumber = null;
        String databaseName = null;
        String username = null;
        String password = null;

        try (InputStreamReader in = new InputStreamReader(System.in); BufferedReader br = new BufferedReader(in)) {
            System.out.print("Enter server name: ");
            serverName = br.readLine();
            System.out.print("Enter port number: ");
            portNumber = br.readLine();
            System.out.print("Enter database name: ");
            databaseName = br.readLine();
            System.out.print("Enter username: ");
            username = br.readLine();
            System.out.print("Enter password: ");
            password = br.readLine();

            System.out.print("Enter the location of the keystore: ");
            // e.g. C:\\Dev\\Always Encrypted\\keystore.jks
            keyStoreLocation = br.readLine();

            System.out.print("Enter the alias of the key stored in the keystore: ");
            // e.g. lp-e796acea-c3bd-4a27-b657-2bb71e3517d1
            keyAlias = br.readLine();

            System.out.print("Enter the password of the keystore and the key: ");
            // <Secret of key Store>
            keyStoreSecret = br.readLine().toCharArray();

            String connectionUrl = "jdbc:sqlserver://" + serverName + ":" + portNumber + ";" + "databaseName="
                    + databaseName + ";username=" + username + ";password=" + password + ";";

            // Open a connection to the database.
            try (Connection connection = DriverManager.getConnection(connectionUrl);
                    Statement stmt = connection.createStatement()) {
                // Instantiate the Java Key Store provider.
                SQLServerColumnEncryptionKeyStoreProvider storeProvider = new SQLServerColumnEncryptionJavaKeyStoreProvider(
                        keyStoreLocation, keyStoreSecret);

                dropKeys(stmt);

                System.out.println();

                /**
                 * Create column mater key For details on syntax refer: https://msdn.microsoft.com/library/mt146393.aspx
                 * 
                 */
                String createCMKSQL = "CREATE COLUMN MASTER KEY " + columnMasterKeyName + " WITH ( "
                        + " KEY_STORE_PROVIDER_NAME = '" + storeProvider.getName() + "' , KEY_PATH =  '" + keyAlias
                        + "' ) ";

                stmt.executeUpdate(createCMKSQL);
                System.out.println("Column Master Key created with name : " + columnMasterKeyName);

                byte[] encryptedCEK = getEncryptedCEK(storeProvider);

                /**
                 * Create column encryption key For more details on the syntax refer:
                 * https://msdn.microsoft.com/library/mt146372.aspx Encrypted CEK first needs to be converted into
                 * varbinary_literal from bytes, for which DatatypeConverter.printHexBinary is used
                 */
                String createCEKSQL = "CREATE COLUMN ENCRYPTION KEY " + columnEncryptionKey + " WITH VALUES ( "
                        + " COLUMN_MASTER_KEY = " + columnMasterKeyName + " , ALGORITHM =  '" + algorithm
                        + "' , ENCRYPTED_VALUE =  0x" + bytesToHexString(encryptedCEK, encryptedCEK.length) + " ) ";

                stmt.executeUpdate(createCEKSQL);
                System.out.println("CEK created with name : " + columnEncryptionKey);
            }
        } catch (Exception e) {
            // Handle any errors that may have occurred.
            e.printStackTrace();
        }
    }

    /**
     * 
     * @param b
     *        byte value
     * @param length
     *        length of the array
     * @return
     */
    final static char[] hexChars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private static String bytesToHexString(byte[] b, int length) {
        StringBuilder sb = new StringBuilder(length * 2);
        for (int i = 0; i < length; i++) {
            int hexVal = b[i] & 0xFF;
            sb.append(hexChars[(hexVal & 0xF0) >> 4]);
            sb.append(hexChars[(hexVal & 0x0F)]);
        }
        return sb.toString();
    }

    private static byte[] getEncryptedCEK(
            SQLServerColumnEncryptionKeyStoreProvider storeProvider) throws SQLServerException {
        /**
         * Following arguments needed by SQLServerColumnEncryptionJavaKeyStoreProvider 1) keyStoreLocation : Path where
         * keystore is located, including the keystore file name. 2) keyStoreSecret : Password of the keystore and the
         * key.
         */
        String plainTextKey = "You need to give your plain text";

        // plainTextKey has to be 32 bytes with current algorithm supported
        byte[] plainCEK = plainTextKey.getBytes();

        // This will give us encrypted column encryption key in bytes
        byte[] encryptedCEK = storeProvider.encryptColumnEncryptionKey(keyAlias, algorithm, plainCEK);

        return encryptedCEK;
    }

    private static void dropKeys(Statement stmt) throws SQLException {
        String cekSql = " if exists (SELECT name from sys.column_encryption_keys where name='" + columnEncryptionKey
                + "')" + " begin" + " drop column encryption key " + columnEncryptionKey + " end";
        stmt.execute(cekSql);

        cekSql = " if exists (SELECT name from sys.column_master_keys where name='" + columnMasterKeyName + "')"
                + " begin" + " drop column master key " + columnMasterKeyName + " end";
        stmt.execute(cekSql);
    }
}
