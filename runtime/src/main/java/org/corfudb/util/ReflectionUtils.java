package org.corfudb.util;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.ClassUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by mwei on 3/29/16.
 */
@Slf4j
public class ReflectionUtils {

    private ReflectionUtils() {
        // prevent instantiation of this class
    }

    static final Map<String, Class> primitiveTypeMap = ImmutableMap.<String, Class>builder()
            .put("int", Integer.TYPE)
            .put("long", Long.TYPE)
            .put("double", Double.TYPE)
            .put("float", Float.TYPE)
            .put("bool", Boolean.TYPE)
            .put("char", Character.TYPE)
            .put("byte", Byte.TYPE)
            .put("void", Void.TYPE)
            .put("short", Short.TYPE)
            .put("int[]", int[].class)
            .put("long[]", long[].class)
            .put("double[]", double[].class)
            .put("float[]", float[].class)
            .put("bool[]", boolean[].class)
            .put("char[]", char[].class)
            .put("byte[]", byte[].class)
            .put("short[]", short[].class)
            .build();
    private static final Pattern methodExtractor = Pattern.compile("([^.\\s]*)\\((.*)\\)$");
    private static final Pattern classExtractor = Pattern.compile("(\\S*)\\.(.*)\\(.*$");

    public static String getShortMethodName(String longName) {
        int packageIndex = longName.substring(0, longName.indexOf('(')).lastIndexOf('.');
        return longName.substring(packageIndex + 1);
    }

    public static String getMethodNameOnlyFromString(String s) {
        return s.substring(0, s.indexOf('('));
    }

    public static Class<?> getPrimitiveType(String s) {
        return primitiveTypeMap.get(s);
    }

    /**
     * Get arguments from method signature.
     * @param s String representation of signature
     * @return Array of types
     */
    public static Class[] getArgumentTypesFromString(String s) {
        String argList = s.contains("(") ? s.substring(s.indexOf('(') + 1, s.length() - 1) : s;
        return Arrays.stream(argList.split(","))
                .filter(x -> !x.equals(""))
                .map(x -> {
                    try {
                        return Class.forName(x);
                    } catch (ClassNotFoundException cnfe) {
                        Class retVal = getPrimitiveType(x);
                        if (retVal == null) {
                            log.warn("Class {} not found", x);
                        }
                        return retVal;
                    }
                })
                .toArray(Class[]::new);
    }

    /**
     * Extract argument types from a string that represents the method
     * signature.
     * @param args Signature string
     * @return Array of types
     */
    public static Class[] getArgumentTypesFromArgumentList(Object[] args) {
        return Arrays.stream(args)
                .map(Object::getClass)
                .toArray(Class[]::new);
    }

    /**
     * Given the constructor arguments and the desired class type, find
     * the closest matching constructor. For example, if a wrapper
     * class contains two constructors, Constructor(A) and Constructor(B),
     * and the constructor argument is of type B, where B inherits from A,
     * instantiate the object via Constructor(B) since it is closes with
     * respect to type hierarchy.
     *
     * @param constructors   Object constructors.
     * @param args           Constructor arguments.
     * @param <T>            Type
     * @return instantiated  wrapper class.
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public static <T> Object findMatchingConstructor(
            Constructor[] constructors, Object[] args)
            throws IllegalAccessException, InvocationTargetException, InstantiationException {
        // Figure out the argument types.
        final List<Class> argTypes = Arrays
                .stream(args).map(Object::getClass)
                .collect(Collectors.toList());
        // Filter out the constructors that do not have the same arity.
        final List<Constructor> candidates = Arrays.stream(constructors)
                .filter(constructor -> constructor.getParameterTypes().length == args.length)
                .collect(Collectors.toList());

        Map<Integer, Constructor> matchingConstructors = new HashMap<>();
        for (Constructor constructor: candidates) {
            final List<Class> constructorTypes = Arrays.asList(constructor.getParameterTypes());
            List<Integer> resolutionList = new LinkedList<>();
            for (int idx = 0; idx < constructorTypes.size(); idx++) {
                int hierarchyDepth = 0; // 0 means unable to match the argument to the type.
                final Class<?> constructorType = constructorTypes.get(idx);
                Class<?> currentType = argTypes.get(idx);
                // If we are able to match the argument type with the constructor parameter
                // increment the hierarchy depth, signaling the distance between the
                // argument type and the parameter type in terms of type hierarchy.
                resolutionList.add(maxDepth(constructorType, currentType, 0));
            }

            // If any of the argument types has type distance of zero, this means
            // that we are unable to match the current constructor with the given arguments.
            if (resolutionList.stream().anyMatch(depth -> depth == 0)) {
                continue;
            }

            // Put all matching constructors in a map in form of:
            // (L1 Norm (Distance) -> Constructor)
            matchingConstructors.put(
                    resolutionList.stream().reduce(0, Integer::sum),
                    constructor);
        }

        // Instantiate the wrapper object with a constructor that has the lowest L1 norm.
        return matchingConstructors.entrySet().stream()
                .max(Map.Entry.comparingByKey()).orElseThrow(
                        () -> new IllegalStateException("No matching constructors found."))
                .getValue().newInstance(args);
    }

    private static int maxDepth(Class constructorType, Class argType, int currentDepth) {
        if (!ClassUtils.isAssignable(argType, constructorType, true)) {
            return currentDepth;
        } else {
            final int newDepth = currentDepth + 1;
            final List<Integer> results = new ArrayList<>();
            final Class[] interfaces = constructorType.getInterfaces();
            final Class superClass = constructorType.getSuperclass();

            Arrays.stream(interfaces)
                    .forEach(clazz -> results.add(maxDepth(interfaces[0], argType, newDepth)));
            Optional.ofNullable(superClass)
                    .map(clazz -> results.add(maxDepth(clazz, argType, newDepth)));
            return results.stream()
                    .max(Comparator.comparing(Integer::valueOf))
                    .orElse(newDepth);
        }
    }

    /**
     * Extract method name from to string path.
     * @param methodString Method signature in the form of a string
     * @return Method object
     */
    public static Method getMethodFromToString(String methodString) {
        Class<?> cls = getClassFromMethodToString(methodString);
        Matcher m = methodExtractor.matcher(methodString);
        m.find();
        try {
            return cls.getDeclaredMethod(m.group(1), getArgumentTypesFromString(m.group(2)));
        } catch (NoSuchMethodException nsme) {
            throw new RuntimeException(nsme);
        }
    }

    /**
     * Extract class name from method to string path.
     * @param methodString toString method path
     * @return String representation of the class name
     */
    public static Class getClassFromMethodToString(String methodString) {
        Matcher m = classExtractor.matcher(methodString);
        m.find();
        String className = m.group(1);
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(cnfe);
        }
    }
}
