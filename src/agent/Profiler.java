package agent;

public class Profiler {
	
	public static void start(String className, String methodName) {
		System.out.println(new StringBuilder(className)
				.append('\t')
				.append(methodName)
				.append("\tstart\t")
				.append(System.currentTimeMillis()));		
	}
	public static void end(String className, String methodName) {
		System.out.println(new StringBuilder(className)
				.append('\t')
				.append(methodName)
				.append("\tend\t")
				.append(System.currentTimeMillis()));		
	}
}