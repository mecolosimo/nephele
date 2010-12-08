/**
 * Created on March 23, 2009.
 *
 * Copyright 2010- The MITRE Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you  may not 
 * use this file except in compliance with the License. You may obtain a copy of 
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions andlimitations under
 * the License.
 *
 * $Id$
 */
package org.mitre.util;

import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Provides a class that can be used to run registered programs similar to the
 * JDBC class drivers and based on Hadoop's ProgramDriver.
 *
 * From <http://java.sun.com/docs/books/tutorial/reflect/class/classNew.html> it would seem that
 * Class.getClasses() Returns all the public classes, interfaces, and enums that are members of the class including inherited members.
 * So, can we have an interface and call MyInterface.class.getClasses()?
 * 
 * @author Marc Colosimo
 */
public class ProgramDriveManager {

    private static final Log LOG = LogFactory.getLog(ProgramDriveManager.class);

    /**
     * A description of a program based on its class and a
     * human-readable description.
     */
    private static Map<String, ProgramDescription> programs;


    static {
        programs = new TreeMap<String, ProgramDescription>();
    }

    /**
     * This is the method that adds the classed to the repository.
     *
     * @param name                The name of the string you want the class instance to be called with
     * @param programDescription  The description of the class
     */
    static public void addClass(String name, ProgramDescription programDescription) {
        if (containsProgram(name)) {
            LOG.warn("Already added '" + name + "' to list!");
        }
        programs.put(name, programDescription);
    }

    static public boolean containsProgram(String name) {
        return programs.containsKey(name);
    }

    static private void printUsage(Map<String, ProgramDescription> programs) {
        System.out.println("Valid program names are:");
        for (Map.Entry<String, ProgramDescription> item : programs.entrySet()) {
            System.out.println("  " + item.getKey() + ": " +
                    item.getValue().getDescription());
        }
    }

    /**
     * This is a driver for the example programs.
     * It looks at the first command line argument and tries to find an
     * example program with that name.
     * If it is found, it calls the main method in that class with the rest
     * of the command line arguments.
     * @param args The argument from the user. args[0] is the command to run.
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws Throwable Anything thrown by the example program's main
     */
    public void driver(String[] args)
            throws Throwable {
        // Make sure they gave us a program name.
        if (args.length == 0) {
            System.err.println("A program must be given as the" +
                    " first argument.");
            printUsage(programs);
            System.exit(-1);
        }

        // And that it is good.
        ProgramDescription pgm = programs.get(args[0]);
        if (pgm == null) {
            System.out.println("Unknown program '" + args[0] + "' chosen.");
            printUsage(programs);
            System.exit(-1);
        }

        // Remove the leading argument and call main
        String[] new_args = new String[args.length - 1];
        for (int i = 1; i < args.length; ++i) {
            new_args[i - 1] = args[i];
        }
        pgm.invoke(new_args);
    }

    /**
     * Generic main. 
     * 
     * Programs that want to be available must register via the static loader.
     *
     * @param args
     */
    static public void main(String[] args) throws Throwable {
        ProgramDriveManager pd = new ProgramDriveManager();
        pd.driver(args);
    }
}
