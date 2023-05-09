public class ScreenLogger {
    private String className;
    private static final String green = "\u001B[32m";
    private static final String red = "\u001B[31m";
    private static final String cyan = "\u001B[36m";
    private static final String black = "\u001B[30m";
    private static final String yellow = "\u001B[33m";
    private static final String reset = "\u001B[0m";

    public ScreenLogger(String className) {
        this.className = className;
    }
    public void info(String message) {
        System.out.println(className + ": " + green + message + reset);
    }

    public void error(String message) {
        System.err.println(className + ": " + red + message + reset);
    }

    public void debug(String message) {
        System.out.println(className + ": " + cyan + message + reset);
    }

    public void warn(String message) {
        System.out.println(className + ": " + yellow + message + reset);
    }

    public static void log(String message) {
        System.out.println(black + ": " + message + reset);
    }

    public static void justInfo(String message) {
        System.out.println(green + message + reset);
    }

    public static void justError(String message) {
        System.err.println(red + message + reset);
    }

    public static void justDebug(String message) {
        System.out.println(cyan + message + reset);
    }

    public static void justWarn(String message) {
        System.out.println(yellow + message + reset);
    }
}
