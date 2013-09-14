
public class TestDumb {


	private static void test1(){
		String a = "asdfasdf";
		StringBuilder sb = new StringBuilder();
		for(int i=a.length()-1;i>-1;i--){
			sb.append(a.charAt(i));
		}
		System.out.println(a);
		System.out.println(sb.toString());

	}
	
	public static void test2(){
		
	}
	
	
	public static void main(String []args){
		
	}
}
