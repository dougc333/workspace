import java.io.File;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

public class Test {

	static class Node {
		int value;
		boolean visited = false;
		Node right;
		Node left;

		public Node() {
		}

		public int getValue() {
			return value;
		}

		public boolean isVisited() {
			return visited;
		}

		public void setVisited(boolean visited) {
			this.visited = visited;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public Node getRight() {
			return right;
		}

		public void setRight(Node right) {
			this.right = right;
		}

		public Node getLeft() {
			return left;
		}

		public void setLeft(Node left) {
			this.left = left;
		}

	}

	public static void getChildNodes(Node n, Queue q) {
		n.visited = true;
		if (n.getLeft() != null) {
			q.add(n.getLeft());
		}
		if (n.getRight() != null) {
			q.add(n.getRight());
		}

	}

	public static void BFS(Node n) {
		Queue<Node> q = new LinkedList<Node>();
		q.add(n);

		while (!q.isEmpty()) {
			Node child = q.remove();
			System.out.println(child.getValue());
			getChildNodes(child, q);
		}
	}

	private static void getDFSChildNodes(Node n, Stack stack) {
		// && n.getLeft().visited == false
		if (n.getLeft() != null) {
			stack.push(n.getLeft());
			// n.getLeft().visited = true;
		}
		// && n.getRight().visited == false
		if (n.getRight() != null) {
			stack.push(n.getRight());
			// n.getRight().visited = true;
		}
	}

	public static void DFS(Node n) {
		Stack<Node> s = new Stack<Node>();
		s.push(n);

		while (!s.isEmpty()) {
			Node child = s.pop();
			System.out.println(child.getValue());
			getDFSChildNodes(child, s);
		}
	}

	// recursive function call is a stack, DFS is easiest
	public static void recursiveDFS(Node n) {
		System.out.println(n.value);
		if (n.getLeft() == null && n.getRight() == null) {
			return;
		}
		recursiveDFS(n.getLeft());
		recursiveDFS(n.getRight());
	}

	public static void recursiveBFS(Node n) {
		Queue<Node> q = new LinkedList();
		q.add(n);
		recursiveDescentBFS(q);
	}

	private static void recursiveDescentBFS(Queue<Node> q) {
		if (q.isEmpty()) {
			return;
		}
		Node processMe = q.remove();
		System.out.println(processMe.getValue());
		if (processMe.getLeft() != null) {
			q.add(processMe.getLeft());
		}
		if (processMe.getRight() != null) {
			q.add(processMe.getRight());
		}
		recursiveDescentBFS(q);
	}

	// I use this in real life.... a DFS starting at a root directory
	public static void recursiveFileListing(File f) {
		System.out.println(f.getAbsolutePath());
		if (f == null) {
			return;
		}
		File listFiles[] = f.listFiles();
		for (File processMe : listFiles) {
			if (processMe.isFile()) {
				recursiveFileListing(processMe);
			}
		}
	}

	public static void main(String[] args) {
		Node one = new Node();
		one.setValue(1);
		Node two = new Node();
		two.setValue(2);
		Node three = new Node();
		three.setValue(3);
		one.setLeft(two);
		one.setRight(three);
		Node four = new Node();
		four.setValue(4);
		Node five = new Node();
		five.setValue(5);
		Node six = new Node();
		six.setValue(6);
		Node seven = new Node();
		seven.setValue(7);
		two.setLeft(four);
		two.setRight(five);
		three.setLeft(six);
		three.setRight(seven);

		// uncomment to run each one
		// BFS(one);
		DFS(one);
		// recursiveBFS(one);
		// recursiveDFS(one);
	}
}
