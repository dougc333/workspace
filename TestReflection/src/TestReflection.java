
public class TestReflection {

	
	private static class A{
		Integer value=10;
		
		public String toString(){
			return "A";
		}
	}
	private static class B{
		String value="asdf";
		
	}
	
	
	
	public static void main(String []args){
		Class testClass = TestReflection.class;
		System.out.println(testClass.toString());
		TestReflection.A a = new A();
		System.out.println(a.toString());
//		System.out.println(testClass.toString());
		Configuration conf = new Configuration();
		//wo setters and getters can you change the value of Configuration? 
		
		//can you get the value of the Hadoop Configuration object? 
		
		
	}
}
