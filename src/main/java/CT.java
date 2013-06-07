package CodeTracer;

public class CT implements AutoCloseable
{
	private static int _indent = 0;

	public CT()
	{
		StackTraceElement ste = Thread.currentThread().getStackTrace()[2];
		_Print("-> " + ste);
		++ _indent;
	}

	@Override
	public void close() {
		-- _indent;
		StackTraceElement ste = Thread.currentThread().getStackTrace()[2];
		_Print("<- " + ste);
	}

	void Print(String s)
	{
		for (int i = 0; i < _indent; ++ i)
			System.out.print("  "); 
		System.out.print(" "); 
		System.out.println(s);
	}

	private void _Print(String s)
	{
		for (int i = 0; i < _indent; ++ i)
			System.out.print("  "); 
		System.out.println(s);
	}

	static void func1() throws Exception
	{
		try (CT ct_ = new CT()) {
			ct_.Print("aaa");
			// throw new Exception();
			func2();
		}
	}

	static void func2() throws Exception
	{
		try (CT ct_ = new CT()) {
			ct_.Print("bbb");
			throw new Exception();
		}
	}

	public static void main(String[] args)
	{
		try
		{
			func1();
		}
		catch (Exception e)
		{
			System.out.println("Caught an exception: " + e);
		}
	}
}
