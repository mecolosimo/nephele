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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A description of a class with a method that has a parameter of <code>String[]</code>
 * <br>
 * Based on Hadoop's ProgramDescription.
 * @author Marc Colosimo
 */
public class ProgramDescription {

    /**
     * String[] args param type.
     */
    static final Class<?>[] paramTypes = new Class<?>[]{String[].class};
    private Method main;
    private String description;
    private String name;

    /**
     * Create a description of class with a main method (a program).
     * <pre>
     * public static void main(String[] args) { }
     * </pre>
     *
     * @param class                     the class with the main.
     * @param name                      the name used to call this description.
     * @param description               a string to display to the user in help messages.
     * @throws SecurityException        if we can't use reflection.
     * @throws NoSuchMethodException    if the class doesn't have a main method.
     */
    public ProgramDescription(Class<?> mainClass, String name,
            String description)
            throws SecurityException, NoSuchMethodException {
        this.main = mainClass.getMethod("main", paramTypes);
        this.name = name;
        this.description = description;
    }

    /**
     * Create a description of class with a main method (a program).
     * <pre>
     * public static void main(String[] args) { }
     * </pre>
     *
     * @param class                     the class to use.
     * @param entryMethod               the method to use instead of main.
     * @param description               a string to display to the user in help messages.
     * @throws SecurityException        if we can't use reflection.
     * @throws NoSuchMethodException    if the class doesn't have a main method.
     */
    public ProgramDescription(Class<?> mainClass, String entryMethod, String name,
            String description)
            throws SecurityException, NoSuchMethodException {
        this.main = mainClass.getMethod(entryMethod, paramTypes);
        this.description = description;
        this.name = name;
    }

    /**
     * Invoke the example application with the given arguments
     * @param args the arguments for the application
     * @throws Throwable The exception thrown by the invoked method
     */
    public void invoke(String[] args) throws Throwable {
        try {
            main.invoke(null, new Object[]{args});
        } catch (InvocationTargetException except) {
            throw except.getCause();
        }
    }

    public String getDescription() {
        return description;
    }

    public String getName() {
        return name;
    }
}


