//this example kinda sucks bc the deadlock is based on time parameters
//have to set work > sleep in main. 
public class TestDeadlock {

	static class SyncThread implements Runnable {
		private Object one;
		private Object two;

		public SyncThread(Object one, Object two) {
			this.one = one;
			this.two = two;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			String name = Thread.currentThread().getName();
			System.out.println("acquriing lock on " + one.getClass());
			synchronized (one) {
				System.out.println(name + "acquired lock on " + one.getClass());
				work();
				System.out.println("acquiring lock on obj2");
				synchronized (two) {
					System.out.println("acquired lock on" + two);
					work();
				}
				System.out.println("released lock on" + two);
			}
			System.out.println("released lock on" + one);
			System.out.println("finished execution");
		}

		private void work() {
			try {
				Thread.sleep(2000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) {
		Object one = new Object();
		Object two = new Object();

		Thread t1 = new Thread(new SyncThread(one, two), "t1");
		Thread t2 = new Thread(new SyncThread(two, one), "t2");
		try {
			t1.start();
			Thread.sleep(1000);
			t2.start();
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
