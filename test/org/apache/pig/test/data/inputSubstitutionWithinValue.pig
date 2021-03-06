/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


%Declare var1 '1'
%DECLARE var2 '6'
%declare total '$var1$var2'
aa = load '/data/intermediate/pow/elcarobootstrap/account/full/weekly/data/$date' using PigStorage('\x01');
bb = filter aa by (ARITY == '$total') and ( $4 eq '' or $4 eq 'NULL' or $4 eq 'ss') parallel 400;
a = foreach bb generate $0,$12,$7;

--generate inactive accts
inactiveAccounts = filter a by ($1 neq '') and ($1 == '2') parallel 400;
store inactiveAccounts into '/user/kaleidoscope/pow_stats/20080228/acct/InactiveAcct';
grpInactiveAcct = group inactiveAccounts all;
countInactiveAcct = foreach grpInactiveAcct { generate COUNT( inactiveAccounts ); }
store countInactiveAcct into '/user/kaleidoscope/pow_stats/20080228/acct_stats/InactiveAcctCount';
