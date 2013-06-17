package CodeTracer;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;


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
      _logger = logger;
      Info(_logger, "_logger was already set. updating.");
      //StackTraceElement[] ste = Thread.currentThread().getStackTrace();
      //for (int i = 2; i < ste.length; i ++)
      //  Info(_logger, " " + ste[i]);
    }
  }

	public CT(Object... objs)
	{
    StringBuilder sb = null;

    for (Object o: objs)
    {
      if (o == null)
        o = "null";

      if (sb == null)
        sb = new StringBuilder();

      if (sb.length() != 0)
        sb.append(", ");

      if (o instanceof List)
      {
        sb.append("[");
        sb.append(StringUtils.join((List<Object>)o, ", "));
        sb.append("]");
      }
      else
        sb.append(o);
    }

    if (sb != null)
      _input = sb.toString();

		StackTraceElement ste = Thread.currentThread().getStackTrace()[2];
		_Info(1, ste);
	}

	@Override
	public void close()
  {
		StackTraceElement ste = Thread.currentThread().getStackTrace()[2];
		_Info(-1, ste);
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
    if (_logger != null)
      _logger.info(_Logstr(s));
    else
      System.err.println(_Logstr(s));
	}

	public void InfoCallStack()
	{
    StackTraceElement[] ste = Thread.currentThread().getStackTrace();
    for (int i = 2; i < ste.length; i ++)
      Info(" " + ste[i]);
	}

	public void Warn(String s)
	{
    if (_logger != null)
      _logger.warn(_Logstr(s));
    else
      System.err.println(_Logstr(s));
  }

	public void Error(String s)
	{
    if (_logger != null)
      _logger.error(_Logstr(s));
    else
      System.err.println(_Logstr(s));
	}

	public void Debug(String s)
	{
    if (_logger != null)
      _logger.debug(_Logstr(s));
    else
      System.err.println(_Logstr(s));
	}

	private static void Info(Logger logger, String s)
	{
    if (logger != null)
      logger.info(_Logstr(s));
    else
      System.err.println(_Logstr(s));
	}

  public void Returns(Object... objs)
  {
    if (objs == null)
    {
      _returns = "null";
      return;
    }

    StringBuilder sb = null;

    for (Object o: objs)
    {
      if (o == null)
        o = "null";

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
        sb.append(o);
    }

    if (sb != null)
      _returns = sb.toString();
  }

	private void _Info(int indinc, StackTraceElement ste)
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

		if (indinc == -1)
		{
			-- ind;
			_ind_map.put(pidtid, ind);
		}

		for (int i = 0; i < ind; ++ i)
      logstr.append("  ");

    if (indinc == 1)
    {
      logstr.append("-> ");
      logstr.append(ste.getClassName());
      logstr.append(".");
      logstr.append(ste.getMethodName());
      logstr.append("(");
      if (_input != null)
        logstr.append(_input);
      logstr.append(") [");
      logstr.append(ste.getFileName());
      logstr.append(":");
      logstr.append(ste.getLineNumber());
      logstr.append("]");
    }
    else if (indinc == -1)
    {
      logstr.append("<- ");
      logstr.append(ste.getClassName());
      logstr.append(".");
      logstr.append(ste.getMethodName());
      logstr.append("()");
      if (_returns != null)
      {
        logstr.append(" ret ");
        logstr.append(_returns);
      }
      logstr.append(" [");
      logstr.append(ste.getFileName());
      logstr.append(":");
      logstr.append(ste.getLineNumber());
      logstr.append("]");
    }
      
    if (_logger != null)
      _logger.info(logstr.toString());
    else
      System.err.println(logstr.toString());

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
