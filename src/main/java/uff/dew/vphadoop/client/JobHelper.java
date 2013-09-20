package uff.dew.vphadoop.client;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;

public class JobHelper {
    private static final Log logger = LogFactory.getLog(JobHelper.class);

    private static Class<?> trackerDistributedCacheManagerClass;

    static {
        if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
            try {
                trackerDistributedCacheManagerClass = Class.forName("org.apache.hadoop.filecache.TrackerDistributedCacheManager");
            } catch (ClassNotFoundException e) {
                trackerDistributedCacheManagerClass = null;
                logger.warn("Unable to provide Windows JAR permission fix: " + e.getMessage(), e);
            }
        }
    }

    public static void hackHadoopStagingOnWin() {
        // do the assignment only on Windows systems
        if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
            // 0655 = -rwxr-xr-x
            JobSubmissionFiles.JOB_DIR_PERMISSION.fromShort((short) 0650);
            JobSubmissionFiles.JOB_FILE_PERMISSION.fromShort((short) 0650);

            if (trackerDistributedCacheManagerClass != null) {
                // handle jar permissions as well
                Field field = findField(trackerDistributedCacheManagerClass, "PUBLIC_CACHE_OBJECT_PERM");
                makeAccessible(field);
                try {
                    FsPermission perm =  (FsPermission) field.get(null);
                    perm.fromShort((short) 0650);

                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Error while trying to set permission on field: " + field, e);
                };
            }
        }
    }

    private static Field findField(Class<?> clazz, String name) {
        Class<?> searchType = clazz;
        while (!Object.class.equals(searchType) && searchType != null) {
            Field[] fields = searchType.getDeclaredFields();
            for (Field field : fields) {
                if ((name == null || name.equals(field.getName()))) {
                    return field;
                }
            }
            searchType = searchType.getSuperclass();
        }
        return null;
    }

    private static void makeAccessible(Field field) {
        if ((!Modifier.isPublic(field.getModifiers()) || !Modifier.isPublic(field.getDeclaringClass().getModifiers()) ||
                Modifier.isFinal(field.getModifiers())) && !field.isAccessible()) {
            field.setAccessible(true);
        }
    }

    public static void copyLocalJarsToHdfs(String localJarsDir, String hdfsJarsDir, Configuration configuration) throws IOException {
        checkRequiredArgument(localJarsDir, "Local JARs dir is null");
        checkRequiredArgument(hdfsJarsDir, "HDFS JARs dir is null");
        checkRequiredArgument(configuration, "Configuration is null");

        Set<File> jarFiles = collectJarFilesFromLocalDir(localJarsDir);

        if (jarFiles.isEmpty()) {
            logger.info("No JAR files found for copying to HDFS under local dir: " + localJarsDir);
        } else {
            logger.info("Copying "+jarFiles.size()+" JAR files from local dir ("+localJarsDir+") to HDFS dir ("+hdfsJarsDir+" at "+resolveHdfsAddress(configuration)+")");
            FileSystem hdfsFileSystem = FileSystem.get(configuration);

            for (File jarFile : jarFiles) {
                Path localJarPath = new Path(jarFile.toURI());
                Path hdfsJarPath = new Path(hdfsJarsDir, jarFile.getName());
                hdfsFileSystem.copyFromLocalFile(false, true, localJarPath, hdfsJarPath);
            }
        }
    }

    private static void checkRequiredArgument(Object argument, String errorMessage) {
        if (argument == null) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    private static Set<File> collectJarFilesFromLocalDir(String localJarsDirPath) {
        File directoryFile = new File(localJarsDirPath);
        if (null == directoryFile) {
            throw new IllegalArgumentException("No directory found at local path: " + localJarsDirPath);
        }
        if (!directoryFile.isDirectory()) {
            throw new IllegalArgumentException("Path points to file, not directory: " + localJarsDirPath);
        }

        Set<File> jarFiles = new HashSet<File>();
        for (File libFile : directoryFile.listFiles()) {
            if (libFile.exists() && !libFile.isDirectory() && libFile.getName().endsWith(".jar")) {
                jarFiles.add(libFile);
            }
        }
        return jarFiles;
    }

    public static void addHdfsJarsToDistributedCache(String hdfsJarsDir, Configuration configuration) throws IOException {
        checkRequiredArgument(hdfsJarsDir, "HDFS JARs dir is null");
        checkRequiredArgument(configuration, "Configuration is null");

        Set<Path> jarPaths = collectJarPathsOnHdfs(hdfsJarsDir, configuration);
        if (!jarPaths.isEmpty()) {
            logger.info("Adding following JARs to distributed cache: " + jarPaths);
            System.setProperty("path.separator", ":"); // due to https://issues.apache.org/jira/browse/HADOOP-9123

            for (Path jarPath : jarPaths) {
                FileSystem jarPathFileSystem = jarPath.getFileSystem(configuration);
                DistributedCache.addFileToClassPath(jarPath, configuration, jarPathFileSystem);
            }
        }
    }

    private static Set<Path> collectJarPathsOnHdfs(String hdfsJarsDir, Configuration configuration) throws IOException {
        Set<Path> jarPaths = new HashSet<Path>();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path jarsDirPath = new Path(hdfsJarsDir);
        if (!fileSystem.exists(jarsDirPath)) {
            throw new IllegalArgumentException("Directory '" + hdfsJarsDir + "' doesn't exist on HDFS (" + resolveHdfsAddress(configuration) + ")");
        }
        if (fileSystem.isFile(jarsDirPath)) {
            throw new IllegalArgumentException("Path '" + hdfsJarsDir + "' on HDFS (" + resolveHdfsAddress(configuration) + ") is file, not directory");
        }

        FileStatus[] fileStatuses = fileSystem.listStatus(jarsDirPath);
        for (FileStatus fileStatus : fileStatuses) {
            if (!fileStatus.isDir()) {
                jarPaths.add(fileStatus.getPath());
            }
        }
        return jarPaths;
    }

    private static String resolveHdfsAddress(Configuration configuration) {
        return configuration.get("fs.default.name");
    }
}
