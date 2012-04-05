/* This file is part of VoltDB.
 * Copyright (C) 2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package voter;

import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.client.exampleutils.PerfCounter;

public interface BenchmarkDBInterface
{
    public void addOptions(AppHelper apph);
    public void getOptions(AppHelper apph);
    public void initializeMain(String servers, int port, ClientStatusListenerExt listener) throws BenchmarkException;
    public Object initializeThread(String servers, int port, ClientStatusListenerExt listener) throws BenchmarkException;
    public void terminateMain();
    public void terminateThread(Object data);
    public int getMaxContestants(int contestantCount, String ContestantNamesCSV) throws BenchmarkException;
    public void updateCall(PhoneCallGenerator.PhoneCall call, int maxVoteCount, Object data, AtomicLongArray votingBoardResults) throws BenchmarkException;
    public PerfCounter getStatistics(String name) throws BenchmarkException;
    public void saveStatistics(String csv) throws BenchmarkException;
    public void getResults(String procName, List<BenchmarkVote> results) throws BenchmarkException;
    public void callProcedure(String procName, boolean ignoreExceptions) throws BenchmarkException;
}
