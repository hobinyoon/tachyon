package CodeTracer;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;
import java.util.Collections;
import java.util.List;


public class CT implements AutoCloseable
{
	private static ConcurrentMap<String, Integer> _ind_map = new ConcurrentHashMap<String, Integer>();

  private static Logger _logger = null;

  String _input = null;
  String _returns = null;

  static public void setLogger(Logger logger)
  {
    if (_logger == null)
      _logger = logger;
    else
    {
      Info(_logger, "_logger was already set");
      StackTraceElement[] ste = Thread.currentThread().getStackTrace();
      for (int i = 2; i < ste.length; i ++)
        Info(_logger, " " + ste[i]);
    }
  }

	public CT(Object... objs)
	{
    StringBuilder sb = null;

    for (Object o: objs)
    {
      if (sb == null)
        sb = new StringBuilder();

      if (sb.length() != 0)
        sb.append(", ");

      // TODO: need to handle collection of collection?
      if (o instanceof List)
      {
        sb.append("[");
        sb.append(StringUtils.join((List<Object>)o, ", "));
        sb.append("]");
      }
      else
      {
        sb.append(o.toString());
      }
    }

    if (sb != null)
      _input = sb.toString();

		StackTraceElement ste = Thread.currentThread().getStackTrace()[2];
		_Info("-> " + ste, 1);
	}

	@Override
	public void close()
  {
		StackTraceElement ste = Thread.currentThread().getStackTrace()[2];
		_Info("<- " + ste, -1);
	}

  private static String _Logstr(String s)
  {
		String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
		Thread t = Thread.currentThread();
		long tid = t.getId();
		String pidtid = pid + " " + tid;
    StringBuilder logstr = new StringBuilder();
    logstr.append(pidtid);
    logstr.append(" ");
    logstr.append(t.getName());
    logstr.append(" | ");

		int ind = 0;
		if (_ind_map.containsKey(pidtid))
			ind = _ind_map.get(pidtid);
		for (int i = 0; i < ind; ++ i)
      logstr.append("  ");
    logstr.append(" ");
    logstr.append(s);

    return logstr.toString();
  }

	public void Info(String s)
	{
    _logger.info(_Logstr(s));
	}

	public void InfoCallStack()
	{
    StackTraceElement[] ste = Thread.currentThread().getStackTrace();
    for (int i = 2; i < ste.length; i ++)
      _logger.info(" " + ste[i]);
	}

	public void Warn(String s)
	{
    _logger.warn(_Logstr(s));
	}

	public void Error(String s)
	{
    _logger.error(_Logstr(s));
	}

	public void Debug(String s)
	{
    _logger.debug(_Logstr(s));
	}

	private static void Info(Logger logger, String s)
	{
    logger.info(_Logstr(s));
	}

  public void Returns(Object... objs)
  {
    StringBuilder sb = null;

    for (Object o: objs)
    {
      if (sb == null)
        sb = new StringBuilder();

      if (sb.length() != 0)
        sb.append(", ");

      // TODO: need to handle collection of collection?
      //if (o instanceof Collections)
      if (o instanceof List)
      {
        sb.append("[");
        sb.append(StringUtils.join((List<Object>)o, ", "));
        sb.append("]");
      }
      else
      {
        sb.append(o.toString());
      }
    }

    if (sb != null)
      _returns = sb.toString();
  }

	private void _Info(String s, int indinc)
	{
    if (_logger == null)
      _logger.info("raise a NullPointerException!");

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
		try (CT _ = new CT()) {
			_.Info("aaa");
			func2();
		}
	}

	static void func2() throws Exception
	{
		try (CT _ = new CT()) {
			_.Info("bbb");
			throw new Exception();
		}
	}

	public static void main(String[] args)
	{
    // A logger needs to be set.
    //CT.setLogger(LOG);

		try (CT _ = new CT()) {
			try
			{
				func1();
			}
			catch (Exception e)
			{
				_.Info("Caught an exception: " + e);
			}
		}
	}
}
