package CodeTracer;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;


public class CT implements AutoCloseable
{
	private static ConcurrentMap<String, Integer> _ind_map = new ConcurrentHashMap<String, Integer>();

  private static Logger _logger;

  String _input = null;
  String _returns = null;

  static public void setLogger(Logger logger)
  {
    _logger = logger;
  }

	public CT(Object... objs)
	{
    for (Object o: objs)
    {
      if (_input == null)
        _input = o.toString();
      else
        _input += (", " + o.toString());
    }

		StackTraceElement ste = Thread.currentThread().getStackTrace()[2];
		_Print("-> " + ste, 1);
	}

	@Override
	public void close() {
		StackTraceElement ste = Thread.currentThread().getStackTrace()[2];
		_Print("<- " + ste, -1);
	}

	public void Print(String s)
	{
		String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
		Thread t = Thread.currentThread();
		long tid = t.getId();
		String pidtid = pid + " " + tid;
		//System.out.print(pidtid + " " + t.getName() + " | ");
    String logstr = pidtid + " " + t.getName() + " | ";

		int ind = 0;
		if (_ind_map.containsKey(pidtid))
			ind = _ind_map.get(pidtid);
		for (int i = 0; i < ind; ++ i)
			//System.out.print("  ");
      logstr += "  ";
		//System.out.print(" ");
    logstr += " ";
		//System.out.println(s);
    logstr += s;

    _logger.info(logstr);
	}

  public void Returns(Object... objs)
  {
    for (Object o: objs)
    {
      if (_returns == null)
        _returns = o.toString();
      else
        _returns += (", " + o.toString());
    }
  }

	private void _Print(String s, int indinc)
	{
		String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
		Thread t = Thread.currentThread();
		long tid = t.getId();
		String pidtid = pid + " " + tid;

		int ind = 0;
		if (_ind_map.containsKey(pidtid))
			ind = _ind_map.get(pidtid);

		if (indinc == -1)
		{
			-- ind;
			_ind_map.put(pidtid, ind);
		}

    String logstr = pidtid + " " + t.getName() + " | ";
		//System.out.print(pidtid + " " + t.getName() + " | ");

		for (int i = 0; i < ind; ++ i)
      logstr += "  ";
			//System.out.print("  ");
		//System.out.println(s);
    logstr += s;
    if (indinc == 1 && _input != null)
      logstr += (" in " + _input);
    else if (indinc == -1 && _returns != null)
      logstr += (" ret " + _returns);
    _logger.info(logstr);

		if (indinc == 1)
		{
			++ ind;
			_ind_map.put(pidtid, ind);
		}
	}

	static void func1() throws Exception
	{
		try (CT ct_ = new CT()) {
			ct_.Print("aaa");
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
		try (CT ct_ = new CT()) {
			try
			{
				func1();
			}
			catch (Exception e)
			{
				ct_.Print("Caught an exception: " + e);
			}
		}
	}
}
