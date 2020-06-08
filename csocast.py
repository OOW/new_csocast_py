# Modified 6/8/2020 by GY: switch status updating time interval from 24 hour to 72 hour
# csocast: version 10.0.0
# Modified 3/13/2020 by GY; Added automatic update of manual RG flag section in Access database file
# Modified 11/25/2019 by GY; Converted Python 2.x code to Python 3.x code
# csocast: version 9.4.1
# Modified 12/27/2017 by EL; Changed SWMM version back to 51011; updated district code (not used)
# Modified 1/31/2017 by EL
# Changed spelling from GAGE to RG_
# Determined hydrographs RG from subcatchments instead of RDII section (2016.01 method)
# Changed model start time to 3 days ago instead of previous 8 days
# csocast: version 9.3.1
# Modified 11/10/2015 by EL
#   - Added manual RG flag option (github issue #3)
# Modified 10/02/2014 by EL
#   - Added the value of the repeated value to the BadDataExplanation column in regs
#   - Added a bad meter and data explanation tracking table into the CSOCast_Archive database

import csv
import glob
import os
import pyodbc
import subprocess
import logging
import re
from datetime import datetime, timedelta


# def uploadOutfile(path, dest):
#    #ftp = ftplib.FTP('ftp.phillywatersheds.org', 'csocast@phillywatersheds.org', 'CastUpdate12')
#    ftp = ftplib.FTP('phillywatersheds.org', 'coop1_all@phillywatersheds.org', 'yzBA3DvypU')
#    ftp.cwd(dest)
#    with open(path, 'r') as f:
#        ftp.storlines('STOR ' + os.path.basename(path), f)
#    ftp.quit()

def getLinkStatusesFromRPTs(cursor, model_dir, status_flags):
    cursor.execute('SELECT DISTINCT District FROM Links')
    csocast_districts = [row.District for row in cursor.fetchall()]
    csocast_links_by_district = {}
    for district in csocast_districts:
        cursor.execute('SELECT Name FROM Links WHERE District = ?', district)
        links = [row.Name for row in cursor.fetchall()]
        csocast_links_by_district[district] = links

    rpt_paths = glob.glob(os.path.join(model_dir, '*.[Rr][Pp][Tt]'))

    model_links_by_district = {}
    link_statuses = []

    for rpt_path in rpt_paths:
        #district = os.path.basename(rpt_path).split('-')[0]
        district = [_f for _f in re.split("[, \-_!?:]+", os.path.basename(rpt_path)) if _f][0]

        if not district in csocast_districts:
            logging.warning('WARNING : %s', 'Skipping .rpt file ' + rpt_path + '. Could not determine district.')
        else:
            cursor.execute('SELECT Name FROM Links WHERE District = ?', (district,))
            csocast_district_links = [row.Name for row in cursor.fetchall()]

            with open(rpt_path, 'r') as f:
                current_line = f.readline()
                while len(current_line) > 0:
                    if not current_line.startswith('  <<< Link'):
                        current_line = f.readline()
                        continue
                    else:
                        linkname = current_line.split()[2]

                        if linkname in csocast_links_by_district[district]:
                            model_links_by_district.setdefault(district, []).append(linkname)
                            num_header_lines = 4
                            for i in range(num_header_lines + 1):
                                current_line = f.readline()

                            last_timestep_had_flow = False
                            status = status_flags['NO_OVERFLOW']
                            while current_line.strip() != '':
                                current_timestep_has_flow = float(current_line.split()[2]) != 0
                                if current_timestep_has_flow and last_timestep_had_flow:
                                    status = status_flags['OVERFLOW_72HRS']
                                    break
                                last_timestep_had_flow = current_timestep_has_flow
                                current_line = f.readline()

                            link_statuses.append({'name': linkname, 'status': status})

                        current_line = f.readline()

    if set(model_links_by_district.keys()) != set(csocast_links_by_district.keys()):
        logging.warning('WARNING : %s',
                        'The districts in the model folder do not match the districts '
                        'listed in the Links table of the csocast database.')

    for district, linknames in csocast_links_by_district.items():
        if not all(linkname in model_links_by_district[district] for linkname in linknames):
            logging.warning('WARNING : %s',
                            "There are some links listed in the csocast database not found in the model output.")

    return link_statuses


def createSWMMInputFromTemplate(template, newfilename, workingRGs, sToday, sTodayTime, startDate):
    headers = ['[TITLE]', '[OPTIONS]', '[FILES]', '[EVAPORATION]', '[RAINGAGES]', '[SUBCATCHMENTS]', '[SUBAREAS]', '[HYDROGRAPHS]',
               '[RDII]', '[CURVES]']

    ofiles = open(template, "r")
    originalData = ofiles.readlines()
    ofiles.close()

    flag = 0

    data = []
    previous = ""
    for row in originalData:
        data.append(row.rstrip())
        if len(row.rstrip()) > 0:
            dataline = row.rstrip().split()
            if dataline[0] in headers:
                data = data[0:-1]
                if previous == "":
                    i = 0
                else:
                    i = headers.index(previous)
                if headers[i] == '[TITLE]':
                    TITLE = data
                elif headers[i] == '[OPTIONS]':
                    Original_OPTIONS = data
                elif headers[i] == '[FILES]':
                    FILES = data
                elif headers[i] == '[EVAPORATION]':
                    EVAPORATION = data
                elif headers[i] == '[SUBCATCHMENTS]':
                    Original_SUBCATCHMENTS = data
                elif headers[i] == '[SUBAREAS]':
                    SUBAREAS = data
                elif headers[i] == '[HYDROGRAPHS]':
                    Original_HYDROGRAPHS = data
                elif headers[i] == '[RDII]':
                    RDII = data
                elif headers[i] == '[RAINGAGES]':
                    Original_RAINGAGES = data

                else:
                    pass

                previous = dataline[0]
                data = []

    CURVES = data

    OPTIONS = []
    for row in Original_OPTIONS:
        if len(row.rstrip()) > 0:
            if row.startswith("START_DATE"):
                row = row.split()
                OPTIONS.append('  '.join((row[0], startDate)))
            elif row.startswith("START_TIME"):
                row = row.split()
                OPTIONS.append('  '.join((row[0], sTodayTime)))
            elif row.startswith("REPORT_START_DATE"):
                row = row.split()
                OPTIONS.append('  '.join((row[0], startDate)))
            elif row.startswith("REPORT_START_TIME"):
                row = row.split()
                OPTIONS.append('  '.join((row[0], sTodayTime)))
            elif row.startswith("END_DATE"):
                row = row.split()
                OPTIONS.append('  '.join((row[0], sToday)))
            elif row.startswith("END_TIME"):
                row = row.split()
                OPTIONS.append('  '.join((row[0], sTodayTime)))
            else:
                OPTIONS.append(row)
        else:
            OPTIONS.append(row)

    RAINGAGES = []
    for row in Original_RAINGAGES:
        if row:
            if row.startswith(';'):
                RAINGAGES.append(row)
            elif row.split()[0] in list(workingRGs.values()):
                RAINGAGES.append(row)

    SUBCATCHMENTS = []
    for row in Original_SUBCATCHMENTS:
        if len(row.rstrip()) > 0:
            rw = row.split()
            if "shed" in rw[0]:
                gage = workingRGs[rw[0]]
                #if gage == 'GAGE999':
                if gage == 'RG_999':
                    flag = 1
                SUBCATCHMENTS.append('  '.join((rw[0], gage, ' '.join(([str(rw[j]) for j in range(2, len(rw))])))))
            else:
                SUBCATCHMENTS.append(row)
        else:
            SUBCATCHMENTS.append(row)

    #2016 model changed name convention
    # RDII section doesn't need to be modified
    # RDII_Patterns = {}
    # for row in RDII:
    #     if len(row.rstrip()) > 0:
    #         rw = row.split()
    #         if "RDII" in rw[0]:
    #             RDII_Patterns[rw[1]] = workingRGs[rw[0]]

    HYDROGRAPHS = []
    for row in Original_HYDROGRAPHS:
        if len(row.rstrip()) > 0:
            rw = row.split()
            #if "GAGE" in rw[1]:
            if "RG_" in rw[1]:
                gage = workingRGs[rw[0][:-4]+'_shed']
                HYDROGRAPHS.append('  '.join((rw[0], gage)))
                #gage = RDII_Patterns[rw[0]]
                #if gage == 'GAGE999':
                if gage == 'RG_999':
                    flag = 1
            else:
                HYDROGRAPHS.append(row)
        else:
            HYDROGRAPHS.append(row)

    ofiles = open(newfilename, "w")

    def writeSWMM(outfile, header, data):
        outfile.write(header + '\n')
        for line in data:
            outfile.write(line + '\n')

        outfile.write('\n')

    writeSWMM(ofiles, '[TITLE]', TITLE)
    writeSWMM(ofiles, '[OPTIONS]', OPTIONS)
    writeSWMM(ofiles, '[FILES]', FILES)
    writeSWMM(ofiles, '[EVAPORATION]', EVAPORATION)
    writeSWMM(ofiles, '[RAINGAGES]', RAINGAGES)
    writeSWMM(ofiles, '[SUBCATCHMENTS]', SUBCATCHMENTS)
    writeSWMM(ofiles, '[SUBAREAS]', SUBAREAS)
    writeSWMM(ofiles, '[HYDROGRAPHS]', HYDROGRAPHS)
    writeSWMM(ofiles, '[RDII]', RDII)
    writeSWMM(ofiles, '[CURVES]', CURVES)

    ofiles.close()

    return True if flag == 0 else False


def getMonitorRegulatorSummary(telog_cursor, reg, status_flags):
    if not reg.Monitored:
        return {'status': status_flags['NOT_MONITORED'], 'lastpoll': datetime.now(),
                'lastoverflow': None, 'explanation': 'Monitored column is False in Regulators table.'}
    elif not reg.Good:
        return {'status': status_flags['NOT_MONITORED'], 'lastpoll': datetime.now(),
                'lastoverflow': None, 'explanation': 'Site marked bad. See comment in BadMeterExplanation.'}
    else:
        telog_cursor.execute("SELECT site_id FROM dbo.sites WHERE site_name = '" + reg.Name + "'")
        site_id_results = telog_cursor.fetchall()

        if len(site_id_results) != 1:

            logging.warning('WARNING : %s', "Can't find site ID for " + reg.Name)
            return {'status': status_flags['NOT_MONITORED'], 'lastpoll': datetime.now(),
                    'lastoverflow': None, 'explanation': 'Unable to obtain site id from Telog'}
        else:
            SITE_ID = str(site_id_results[0].site_id)

            # if site id exists, get measurement ids for trunk level, sw level, and gate percentage
            # get trunk measurement_id for SITE_ID
            telog_cursor.execute("SELECT measurement_id FROM dbo.measurements WHERE site_id = ?"
                                 " AND measurement_name = ?", (SITE_ID, reg.TrunkLevelName))
            trunk_id_results = telog_cursor.fetchall()
            if len(trunk_id_results) != 1:
                logging.warning(
                    "WARNING %s :', 'Can't find measurement '" + reg.TrunkLevelName + "' for " + reg.Name + ' (' + str(
                        SITE_ID) + ')')
                return {'status': status_flags['NOT_MONITORED'], 'lastpoll': datetime.now(),
                        'lastoverflow': None, 'explanation': 'Unable to find trunk level measurement ID in Telog'}
            else:
                TRUNK_ID = str(trunk_id_results[0].measurement_id)

                # get SWO measurement_id for SITE_ID
                telog_cursor.execute("SELECT measurement_id FROM dbo.measurements WHERE site_id = ?"
                                     " AND measurement_name = ?", (SITE_ID, reg.SWOLevelName))
                swo_id_results = telog_cursor.fetchall()
                if len(swo_id_results) != 1 and reg.Type != 'nontidal':
                    logging.warning("WARNING %s",
                                    "Can't find measurement '" + reg.SWOLevelName + "' for " + reg.Name + ' (' + str(
                                        SITE_ID) + ')')
                    return {'status': status_flags['NOT_MONITORED'], 'lastpoll': datetime.now(),
                            'lastoverflow': None, 'explanation': 'Unable to find SWO level measurement ID in Telog.'}
                else:
                    SWO_ID = str(swo_id_results[0].measurement_id) if reg.Type != 'nontidal' else str(-1)

                    # get SWO gate measurment_id for SITE_ID
                    telog_cursor.execute("SELECT measurement_id FROM dbo.measurements WHERE site_id = ?"
                                         " AND measurement_name = ?", (SITE_ID, reg.GatePositionName))
                    gate_id_results = telog_cursor.fetchall()
                    if reg.Type == 'cc' and len(gate_id_results) != 1:
                        logging.warning("WARNING %s",
                                     "Can't find measurement '" + reg.GatePostionName + "' for " +
                                     reg.Name + ' (' + str(
                                         SITE_ID) + ')')
                        return {'status': status_flags['NOT_MONITORED'], 'lastpoll': datetime.now(),
                                'lastoverflow': None,
                                'explanation': 'Unable to find gate position measurement ID in Telog.'}

                    else:
                        # if reg is not cc, assign an impossible gate id. the following query is general,
                        # and works for both cc and non-cc regs
                        GATE_ID = str(gate_id_results[0].measurement_id) if reg.Type == 'cc' else str(-1)

                        # if required measurements exist, get the data and process it
                        # Create temporary #TrendData table, with columns DateTime, TRL, SWL[, SWGT].
                        # The LEFT OUTER JOIN on Gate means that Gate.SWGT will be all NULLs if the regulator
                        # is not computer controlled
                        telog_cursor.execute("SELECT OBJECT_ID('tempdb..#TrendData')")
                        if telog_cursor.fetchone()[0] is not None:
                            telog_cursor.execute('DROP TABLE #TrendData')

                        telog_cursor.execute("""
                            WITH Trunk AS (
                                SELECT trend_data_time AS DateTime, trend_data_avg AS TRL
                                FROM dbo.trend_data
                                WHERE measurement_id = """ + TRUNK_ID + """ AND trend_data_time >= DATEADD(day, -3, GETDATE())
                            ),
                            SWO AS (
                                SELECT trend_data_time AS DateTime, trend_data_avg AS SWL
                                FROM dbo.trend_data
                                WHERE measurement_id = """ + SWO_ID + """ AND trend_data_time >= DATEADD(day, -3, GETDATE())
                            ),
                            Gate AS (
                                SELECT trend_data_time AS DateTime, trend_data_avg AS SWGT
                                FROM dbo.trend_data
                                WHERE measurement_id = """ + GATE_ID + """ AND trend_data_time >= DATEADD(day, -3, GETDATE())
                            )
                            SELECT Trunk.DateTime,
                                   Trunk.TRL,
                                   SWO.SWL,
                                   Gate.SWGT
                            INTO #TrendData
                            FROM Trunk LEFT OUTER JOIN SWO
                                  ON Trunk.DateTime = SWO.DateTime
                                 LEFT OUTER JOIN Gate
                                  ON Trunk.DateTime = Gate.DateTime
                        """)

                        # get last dtime
                        telog_cursor.execute('SELECT Max(DateTime) AS MaxDateTime FROM #TrendData')
                        lastpoll = telog_cursor.fetchone().MaxDateTime

                        if lastpoll is None:
                            # sensor is out for at least a day -> bad data
                            telog_cursor.execute(
                                "SELECT MAX(trend_data_time) AS lastpoll FROM dbo.trend_data WHERE measurement_id = " +
                                TRUNK_ID)
                            return {'status': status_flags['BAD_DATA'], 'lastpoll': telog_cursor.fetchone().lastpoll,
                                    'lastoverflow': None, 'explanation': 'Data is more than a day old.'}
                        elif lastpoll <= (datetime.now() - timedelta(hours=5)):
                            # sensor is out for less than a day but more than 5 hours, assume due to overflow.
                            # Set status to BAD_DATA and lastoverflow to now
                            return {'status': status_flags['BAD_DATA'], 'lastpoll': lastpoll,
                                    'lastoverflow': lastpoll, 'explanation': 'Data is more than 5 hours old.'}
                        else:
                            # data filter: if there are values repeated more than 288 times in the last day,
                            # then the data is bad
                            telog_cursor.execute("""
                                SELECT COUNT(*) AS RecordCount
                                FROM #TrendData
                            """)
                            record_count = telog_cursor.fetchone().RecordCount

                            # If 2.5 minute data is used, then num_steps_in_12hrs equals 288
                            num_steps_in_12hrs = (12 * 60) // float(reg.TimeStep_mins)

                            telog_cursor.execute("""
                                SELECT TRL, COUNT(TRL) AS TRLCount
                                FROM #TrendData
                                GROUP BY TRL
                                HAVING COUNT(TRL) > ?
                            """, num_steps_in_12hrs)
                            has_repeated_values = telog_cursor.fetchall()

                            # code.interact(local=locals())

                            # split this section into two parts, 9/26/2014 EL
                            # if record_count < num_steps_in_12hrs or has_repeated_values:
                            #    return {'status' : status_flags['BAD_DATA'], 'lastpoll' : lastpoll,
                            #            'lastoverflow' : None, 'explanation' : 'Data failed the quality filter.'}
                            if record_count < num_steps_in_12hrs:
                                return {'status': status_flags['BAD_DATA'], 'lastpoll': lastpoll,
                                        'lastoverflow': None,
                                        'explanation': 'Data failed the quality filter; missing data.'}
                            elif has_repeated_values:
                                repeated_value = str(round(float(str(has_repeated_values[0]).split(',')[0][1:]), 3))
                                return {'status': status_flags['BAD_DATA'], 'lastpoll': lastpoll,
                                        'lastoverflow': None,
                                        'explanation': 'Data failed the quality filter; repeated values = ' +
                                                       repeated_value}

                            else:  # data passes filter
                                # check timestep assumption
                                telog_cursor.execute('SELECT DateTime FROM #TrendData ORDER BY DateTime')
                                datetimes = [row.DateTime for row in telog_cursor.fetchall()]
                                min_timestep_secs = min([(dtime2 - dtime1).total_seconds() for dtime1, dtime2 in
                                                         zip(datetimes[:-1], datetimes[1:])])
                                assumed_timestep_secs = float(reg.TimeStep_mins) * 60
                                if min_timestep_secs < assumed_timestep_secs:
                                    logging.warn('WARNING %s:',
                                                 reg.Name + ' violates the timestep assumption. Timestep of ' + str(
                                                     min_timestep_secs // 60) + ' encountered.')
                                if reg.Type == 'slot':
                                    telog_cursor.execute("SELECT SWL FROM #TrendData WHERE NOT SWL IS NULL")
                                    if not telog_cursor.fetchone():
                                        return {'status': status_flags['BAD_DATA'], 'lastpoll': lastpoll,
                                                'lastoverflow': None, 'explanation': 'SWL not reporting.'}
                                    else:
                                        telog_cursor.execute("""
                                            SELECT DateTime, TRL, SWL
                                            FROM #TrendData
                                            ORDER BY SWL
                                        """)
                                        medianrange = telog_cursor.fetchall()
                                        trl_medianrange = sorted([row.TRL for row in medianrange if row.TRL])
                                        swl_medianrange = sorted([row.SWL for row in medianrange if row.SWL])

                                        count_trl = len(trl_medianrange)
                                        count_swl = len(swl_medianrange)

                                        median_TRL = 99999
                                        if trl_medianrange:
                                            if count_trl == 1:
                                                median_TRL = trl_medianrange[0]
                                            elif (count_trl % 2) == 0:
                                                middle_ix = (count_trl // 2) - 1
                                                median_TRL = (trl_medianrange[middle_ix] + trl_medianrange[
                                                    middle_ix + 1]) // 2
                                            else:
                                                middle_ix = ((count_trl - 1) // 2) + 1
                                                median_TRL = trl_medianrange[middle_ix]

                                        median_SWL = 99999
                                        if swl_medianrange:
                                            if count_swl == 1:
                                                median_SWL = swl_medianrange[0]
                                            elif (count_swl % 2) == 0:
                                                middle_ix = (count_swl // 2) - 1
                                                median_SWL = (swl_medianrange[middle_ix] + swl_medianrange[
                                                    middle_ix + 1]) // 2
                                            else:
                                                middle_ix = ((count_swl - 1) // 2) + 1
                                                median_SWL = swl_medianrange[middle_ix]

                                        median_TRL = str(median_TRL * 2)
                                        median_SWL = str(median_SWL)
                                        telog_cursor.execute("""
                                            SELECT MAX(DateTime) AS MaxDateTime
                                            FROM #TrendData
                                            WHERE TRL > """ + median_TRL + " AND SWL > " + median_SWL
                                                             )
                                elif reg.Type == 'tidal':

                                    # Download SWL data and if missing return bad data; often data is bad
                                    telog_cursor.execute("SELECT SWL FROM #TrendData WHERE NOT SWL IS NULL")
                                    if not telog_cursor.fetchone():
                                        return {'status': status_flags['BAD_DATA'], 'lastpoll': lastpoll,
                                                'lastoverflow': None, 'explanation': 'SWL not reporting.'}
                                    else:
                                        telog_cursor.execute("""
                                            WITH Elevations AS (
                                                SELECT DateTime,
                                                       TRL / 12 + ? - ? AS TRL_el,
                                                       SWL / 12 + ? - ? AS SWL_el
                                                    FROM #TrendData
                                            )
                                            SELECT Max(DateTime) AS MaxDateTime
                                            FROM Elevations
                                            WHERE TRL_el > ? AND TRL_el > SWL_el
                                        """, (reg.TrunkInvert, reg.TRL_Offset,
                                              reg.OFInvert, reg.SWO_Offset,
                                              reg.DamInvert))

                                elif reg.Type == 'nontidal':
                                    telog_cursor.execute("""
                                        WITH Elevation AS (
                                            SELECT DateTime,
                                                   TRL / 12 + ? - ? AS TRL_el
                                            FROM #TrendData
                                        )
                                        SELECT MAX(DateTime) AS MaxDateTime
                                        FROM Elevation
                                        WHERE TRL_el > ?
                                    """, (reg.TrunkInvert, reg.TRL_Offset, reg.DamInvert))
                                elif reg.Type == 'cc':
                                    telog_cursor.execute("SELECT SWGT FROM #TrendData WHERE NOT SWGT IS NULL")
                                    if not telog_cursor.fetchone():
                                        return {'status': status_flags['BAD_DATA'], 'lastpoll': lastpoll,
                                                'lastoverflow': None, 'explanation': 'SWGT not reporting.'}
                                    else:
                                        telog_cursor.execute("""
                                            SELECT MAX(DateTime) AS MaxDateTime
                                            FROM #TrendData
                                            WHERE SWGT > 5 AND TRL > ?
                                        """, reg.CC_Open)

                                results = telog_cursor.fetchone()
                                lastoverflow = results.MaxDateTime if results else None

                                if lastoverflow:
                                    secs_hour = 60 * 60
                                    if (datetime.now() - lastoverflow).total_seconds() // secs_hour < 2:
                                        status = status_flags['OVERFLOW_CURRENT']
                                    else:
                                        status = status_flags['OVERFLOW_72HRS']
                                else:
                                    status = status_flags['NO_OVERFLOW']
                                return {'status': status, 'lastpoll': lastpoll, 'lastoverflow': lastoverflow,
                                        'explanation': None}


def getRainGageSummaryAndStoreData(cursor, rg, raindata_dir):
    # get site_id for rain['Name']
    cursor.execute("SELECT site_id FROM dbo.sites WHERE site_name = '" + rg.Name + "'")
    site_id_results = cursor.fetchall()
    if len(site_id_results) != 1:
        logging.warning("WARNING : %s", "Can't find site ID for raingage " + rg.Name)
        return {'lastpoll': datetime.now(), 'volume': -1, 'peakintensity': -1}

    SITE_ID = str(site_id_results[0].site_id)

    # get tips measurement_id for SITE_ID
    tips_measurement_name = 'RAIN FALL TIPS'
    cursor.execute(
        "SELECT measurement_id FROM dbo.measurements WHERE site_id = " + SITE_ID + " AND measurement_name = '" +
        tips_measurement_name + "'")
    tips_id_results = cursor.fetchall()
    if len(tips_id_results) != 1:
        logging.warning("WARNING : %s", "Can't find measurement '" + tips_measurement_name + "' for " + rg.Name)
        return {'lastpoll': datetime.now(), 'volume': -1, 'peakintensity': -1}
    TIPS_ID = str(tips_id_results[0].measurement_id)

    cursor.execute("SELECT OBJECT_ID('tempdb..#TrendData')")
    if cursor.fetchone()[0]:
        cursor.execute('DROP TABLE #TrendData')
    cursor.execute("""
        SELECT trend_data_time AS DateTime, trend_data_avg AS Tips
        INTO #TrendData
        FROM dbo.trend_data
        WHERE measurement_id = """ + TIPS_ID + """ AND trend_data_time >= DATEADD(week, -2, GETDATE())
    """)

    cursor.execute("SELECT MAX(DateTime) AS MaxDateTime FROM #TrendData")
    last_dtime = cursor.fetchone().MaxDateTime

    if last_dtime:
        cursor.execute("""
            SELECT '""" + str(rg.Number) + """' AS Gage,
              DATEPART(year, DateTime) AS Year,
              DATEPART(month, DateTime) AS Month,
              DATEPART(day, DateTime) AS Day,
              DATEPART(hour, DateTime) AS Hour,
              DATEPART(minute, DateTime) AS Minute,
              Tips / 100 AS Rain
            FROM #TrendData
            ORDER BY DateTime
        """)

        results = cursor.fetchall()
        with open(os.path.join(raindata_dir, rg.Name + '.txt'), 'w') as f:
            for row in results:
                row_fmted = '{:<4}{:<7}{:<5}{:<5}{:<5}{:<4}{:<5.3}'.format(*row)
                f.write(row_fmted + '\n')
    else:
        logging.warning("WARNING : %s", "Data for " + rg.Name + " is more than two weeks old.")
        return {'lastpoll': datetime.now(), 'volume': -1, 'peakintensity': -1, 'good': False}

    if last_dtime >= datetime.now() - timedelta(days=2):
        cursor.execute("""
            SELECT MAX(DateTime) AS lastpoll,
                   SUM(Tips) / 100 AS volume,
                   MAX(Tips) / 100 AS peakintensity
            FROM #TrendData
            WHERE DateTime >= DATEADD(day, -2, GETDATE())
        """)
        result = cursor.fetchone()
        result_names = [desc[0] for desc in result.cursor_description]
        return dict(list(zip(result_names, result)) + [('good', True)])
    else:
        return {'lastpoll': datetime.now(), 'volume': -1, 'peakintensity': -1, 'good': False}


def setupAndRunModels(cursor, model_dir):
    cursor.execute('SELECT Name FROM Subcatchments')
    subcatchments = [row.Name for row in cursor.fetchall()]

    working_gages_by_subcatchment = {}
    for subcatchment in subcatchments:
        cursor.execute('SELECT Gage0, Gage1, Gage2 FROM Subcatchments WHERE Name = ?', subcatchment)
        gages = cursor.fetchone()
        working_gage_found = False
        current_gage_num = 0
        max_number_of_gages = 3
        while (not working_gage_found) and (current_gage_num < max_number_of_gages):
            cursor.execute("""
                SELECT Good, Manual_Flag
                FROM RainGages
                WHERE ModelName = ?
            """, gages[current_gage_num])
            gage_is_good = cursor.fetchone()
            # RG is only good if both good and manual flag are good
            gage_is_good = gage_is_good[0] and not gage_is_good[1]

            if gage_is_good:
                cursor.execute("""
                    UPDATE Subcatchments
                    SET LastGageUsed = ?
                    WHERE Name = ?
                """, (current_gage_num, subcatchment))
                working_gages_by_subcatchment[subcatchment] = gages[current_gage_num]
                if current_gage_num > 0:
                    logging.warning('WARNING : %s', 'Backup ' + gages[current_gage_num] \
                                    + ' used for subcatchment ' + subcatchment + '.')

                working_gage_found = True
            else:
                current_gage_num += 1

        if not working_gage_found:
            msg = 'No working raingages found for ' + subcatchment + '.'
            #working_gages_by_subcatchment[subcatchment] = 'GAGE999'
            working_gages_by_subcatchment[subcatchment] = 'RG_999'
            logging.warning('WARNING : %s', msg)

    model_template_dir = os.path.join(model_dir, 'inp_templates')
    inp_template_paths = glob.glob(os.path.join(model_template_dir, '*.[Ii][Nn][Pp]'))

    for path in glob.glob(os.path.join(model_dir, '*.[Ii][Nn][Pp]')):
        os.unlink(path)
    for path in glob.glob(os.path.join(model_dir, '*.[Rr][Pp][Tt]')):
        os.unlink(path)

    todaydate_str = datetime.now().strftime("%m/%d/%Y")
    todaytime_str = datetime.now().strftime("%H:00:00")
    #yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y")
    #weekago_str = (datetime.now() - timedelta(days=8)).strftime("%m/%d/%Y")
    ThreeDaysAgo_str = (datetime.now() - timedelta(days=3)).strftime("%m/%d/%Y")
    #swmm_exe_path = os.path.join(model_dir, 'swmm5_022.exe')
    swmm_exe_path = os.path.join(model_dir, 'swmm51011.exe')
    #swmm_exe_path = os.path.join(model_dir, 'swmm51012.exe')
    for inp_template_path in inp_template_paths:
        new_inp_fname = os.path.splitext(os.path.basename(inp_template_path))[0] + '_' \
                        + datetime.now().strftime('%Y%m%d') + '.inp'
        new_inp_path = os.path.join(model_dir, new_inp_fname)

        ok_to_run = createSWMMInputFromTemplate(inp_template_path, new_inp_path, working_gages_by_subcatchment,
                                                todaydate_str, todaytime_str, ThreeDaysAgo_str)

        rpt_path = os.path.join(model_dir, os.path.splitext(new_inp_fname)[0] + '.rpt')

        if ok_to_run:
            # changing directory is necessary for the paths in the .inp files to be correct
            cwd = os.getcwd()
            os.chdir(model_dir)
            subprocess.call('"' + swmm_exe_path + '" "' + new_inp_path + '" "' + rpt_path + '"')
            os.chdir(cwd)


def tidedata_good(model_dir):
    timeseries_dir = os.path.join(model_dir, 'timeseries')
    for fpath in glob.glob(os.path.join(timeseries_dir, '*.dat')):
        with open(fpath, 'r') as f:
            reader = csv.reader(f, delimiter=' ', skipinitialspace=True)
            max_dtime = max([datetime.strptime(row[0], '%m/%d/%Y') for row in reader])
            if max_dtime < datetime.now() + timedelta(days=7):
                return False

    return True


def csocast(working_dir, upload=True, run_model=True, backup=True):
    status_flags = {'NOT_MONITORED': 0,
                    'NO_OVERFLOW': 1,
                    'BAD_DATA': 2,
                    'OVERFLOW_72HRS': 3,
                    'OVERFLOW_CURRENT': 4}
    pubmsg_fname = 'public_message.txt'

    # abspath is necessary for the cnxn string to work
    csocast_db_path = os.path.abspath(os.path.join(working_dir, 'csocast.accdb'))
    csocast_db = pyodbc.connect('Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + csocast_db_path,
                                autocommit=True)
    # pyodbc.connect('Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + csocast_db_path,autocommit=True)
    csocast_cursor = csocast_db.cursor()

    # Server relocated 11/16/2017 from PWDRTC to pwdfoxsql
    # telog_db = pyodbc.connect('Driver={SQL Server};SERVER=PWDRTC;UID=readonly;PWD=readonly;DATABASE=Telog',
    #                           autocommit=True)
    telog_db = pyodbc.connect('Driver={SQL Server};SERVER=170.115.87.100;UID=readonly;PWD=readonly;DATABASE=Telog',
                              autocommit=True)
    telog_cursor = telog_db.cursor()

    csocast_cursor.execute('SELECT Name, Number FROM RainGages')
    raingages = csocast_cursor.fetchall()

    model_dir = os.path.join(working_dir, 'model')

    output_dir = os.path.join(working_dir, 'output')
    rain_outpath = os.path.join(output_dir, 'rain_out.txt')
    raindata_dir = os.path.join(model_dir, 'rainfall')
    # for fname in os.listdir(raindata_dir):
    #    os.unlink(os.path.join(raindata_dir, fname))
    csocast_cursor.execute('UPDATE RainGages SET LastPoll = NULL, Volume_2day = NULL, PeakIntensity_2day = NULL')
    # Get raingages summary from Telog
    for rg in raingages:

        summary = getRainGageSummaryAndStoreData(telog_cursor, rg, raindata_dir)
        csocast_cursor.execute("""
            UPDATE RainGages
            SET LastPoll = ?,
                Volume_2day = ?,
                PeakIntensity_2day = ?,
                Good = ?
            WHERE Name = ?
        """, (summary['lastpoll'], summary['volume'], summary['peakintensity'], summary['good'], rg.Name))

    # Flag out the wrong raingauge    
    RainTrue_Flag = 0
    RainFalse_Flag = 0
    sumT_volume_2day = 0
    csocast_cursor.execute(
        "SELECT Name, Manual_Flag, Good, Volume_2day, LastPoll, Lat, Long, PeakIntensity_2day FROM RainGages")
    rain_summaries = csocast_cursor.fetchall()
    for summary in rain_summaries:
        if summary.Volume_2day > 0:
            RainTrue_Flag = RainTrue_Flag + 1
            sumT_volume_2day = sumT_volume_2day + summary.Volume_2day
        elif summary.Volume_2day < 0:
            csocast_cursor.execute("""
                                   UPDATE RainGages
                                   SET Manual_Flag = True
                                   """)
        else:
            RainFalse_Flag = RainFalse_Flag + 1
            csocast_cursor.execute("""
                                   UPDATE RainGages
                                   SET Manual_Flag = False
                                   """)
    if RainFalse_Flag > 22:
        for summary in rain_summaries:
            if summary.Volume_2day > 0.5:
                csocast_cursor.execute("""
                                       UPDATE RainGages
                                       SET Manual_Flag = True
                                       """)
    else:
        csocast_cursor.execute("""
                               UPDATE RainGages
                               SET Manual_Flag = False
                               """)

    csocast_cursor.execute("""
        UPDATE Regulators
        SET LastMonitorStatus = NULL, LastPoll = NULL, LastOverFlow_72hrs = NULL, BadDataExplanation = NULL
    """)
    csocast_cursor.execute("""
        SELECT Name, Type, Monitored, TrunkLevelName, SWOLevelName, GatePositionName,
            TimeStep_mins, TrunkInvert, TRL_Offset, DamInvert, OFInvert, SWO_Offset, CC_Open, Good
        FROM Regulators
    """)
    regulators = csocast_cursor.fetchall()

    # Get regulator summary from Telog
    for reg in regulators:

        summary = getMonitorRegulatorSummary(telog_cursor, reg, status_flags)
        csocast_cursor.execute("""
            UPDATE Regulators
            SET LastMonitorStatus = ?,
                LastPoll = ?,
                LastOverFlow_72hrs = ?,
                BadDataExplanation = ?
            WHERE Name = ?
        """, (summary['status'], summary['lastpoll'], summary['lastoverflow'], summary['explanation'], reg.Name))


    model_dir = os.path.join(working_dir, 'model')

    output_dir = os.path.join(working_dir, 'output')
    hybrid_outpath = os.path.join(output_dir, 'hybrid_out.txt')
    rain_outpath = os.path.join(output_dir, 'rain_out.txt')
    if not tidedata_good(model_dir):
        logging.warning("ERROR : %s", "Model tide data out-of-date.")
        with open(hybrid_outpath, 'w') as f:
            f.write('\nSystem Msg,CSOCast is temporarily down for maintenance. Please check back later.')
        # uploadOutfile(hybrid_outpath, '/castdata/')
        with open(rain_outpath, 'w') as f:
            f.write('')
            # uploadOutfile(rain_outpath, '/castdata/')
    else:
        setupAndRunModels(csocast_cursor, model_dir)
        link_statuses = getLinkStatusesFromRPTs(csocast_cursor, model_dir, status_flags)
        status_timestamp = datetime.now()
        for link_status in link_statuses:
            csocast_cursor.execute("""
                UPDATE Links
                SET LastStatus = ?,
                    LastStatusTimeStamp = ?
                WHERE Name = ?
            """, link_status['status'], status_timestamp, link_status['name'])
        csocast_cursor.execute('UPDATE Regulators SET LastModelStatus = NULL')
        csocast_cursor.execute('SELECT Regulator, Max(LastStatus) AS MaxLinkLastStatus FROM Links GROUP BY Regulator')
        model_regulator_statuses = csocast_cursor.fetchall()
        for status in model_regulator_statuses:
            csocast_cursor.execute("""
                UPDATE Regulators
                SET LastModelStatus = ?
                WHERE Name = ?
            """, status.MaxLinkLastStatus, status.Regulator)

        csocast_cursor.execute("""
            UPDATE Outfalls
            SET LastMonitorStatus = NULL, LastModelStatus = NULL, LastPoll = NULL, LastHybridStatus = NULL
        """)
        csocast_cursor.execute("""
            SELECT Outfall,
                   Max(LastMonitorStatus) AS MaxLastMonitorStatus,
                   Max(LastModelStatus) AS MaxLastModelStatus,
                   Max(LastPoll) AS MaxLastPoll
            FROM Regulators
            WHERE OverFlowDeterminant = 1
            GROUP BY Outfall
        """)
        outfall_statuses = csocast_cursor.fetchall()

        for status in outfall_statuses:
            mon_status = status.MaxLastMonitorStatus
            mod_status = status.MaxLastModelStatus
            if mon_status == status_flags['NOT_MONITORED']:
                hybrid_status = mon_status
            elif mon_status != status_flags['BAD_DATA']:
                if mon_status == mod_status or (not (not (mon_status == status_flags['OVERFLOW_CURRENT']) or not (
                            mod_status == status_flags['OVERFLOW_72HRS']))):
                    hybrid_status = mon_status
                else:
                    logging.warning('WARNING %s:', 'Model and monitor disagree for outfall ' + status.Outfall)
                    hybrid_status = status_flags['NOT_MONITORED']
            else:
                hybrid_status = status_flags['NOT_MONITORED']

            csocast_cursor.execute("""
                UPDATE Outfalls
                SET LastMonitorStatus = ?,
                    LastModelStatus = ?,
                    LastPoll = ?,
                    LastHybridStatus = ?
                WHERE Name = ?
            """, status.MaxLastMonitorStatus, status.MaxLastModelStatus, status.MaxLastPoll, hybrid_status,
                                   status.Outfall)

        csocast_cursor.execute("""
            SELECT Name, LastHybridStatus, LastMonitorStatus, LastModelStatus, LastPoll, Lat,
                Long, Interceptor, Waterbody, Image, Street, Representative
            FROM Outfalls
            ORDER BY Name
        """)
        summaries = csocast_cursor.fetchall()
        status_messages = {status_flags['OVERFLOW_CURRENT']: "Currently overflowing.",
                           status_flags['OVERFLOW_72HRS']: "Overflow in the past 72 hours.",
                           status_flags['BAD_DATA']: 'Data is not currently available.',
                           status_flags['NO_OVERFLOW']: 'No overflow in the past 72 hours.',
                           status_flags['NOT_MONITORED']: 'Data is not currently available.'}
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        monitor_outpath = os.path.join(output_dir, 'monitor_out.txt')
        model_outpath = os.path.join(output_dir, 'model_out.txt')
        with open(hybrid_outpath, 'w') as hybrid_f:
            hybrid_writer = csv.writer(hybrid_f, lineterminator='\n')
            with open(monitor_outpath, 'w') as monitor_f:
                monitor_writer = csv.writer(monitor_f, lineterminator='\n')
                with open(model_outpath, 'w') as model_f:
                    model_writer = csv.writer(model_f, lineterminator='\n')
                    for summ in summaries:
                        output_values1 = [summ.LastPoll.strftime('%m/%d/%Y %H:%M:%S'), summ.Lat, summ.Long]
                        output_values2 = [summ.Interceptor, summ.Waterbody, summ.Image, summ.Street,
                                          'R' if summ.Representative else '']
                        monitor_row = [summ.Name] + [status_messages[summ.LastMonitorStatus]] + output_values1 \
                                      + [summ.LastMonitorStatus] + output_values2
                        model_row = [summ.Name] + [status_messages[summ.LastModelStatus]] + output_values1 \
                                    + [summ.LastModelStatus] + output_values2
                        hybrid_row = [summ.Name] + [status_messages[summ.LastHybridStatus]] + output_values1 \
                                     + [summ.LastHybridStatus] + output_values2
                        monitor_writer.writerow(monitor_row)
                        model_writer.writerow(model_row)
                        hybrid_writer.writerow(hybrid_row)
                    for f in monitor_f, model_f, hybrid_f:
                        good_summaries = [row for row in summaries
                                          if row.LastMonitorStatus not in [status_flags['BAD_DATA'],
                                                                           status_flags['NOT_MONITORED']]]

                        if good_summaries:
                            lastupdate = min(row.LastPoll for row in good_summaries)
                            f.write('Data last updated, ' + lastupdate.strftime('%m/%d/%Y %I:%M %p'))
                        else:
                            msg = ('There are no good sites right now. See column [BadDataExplanation] in '
                                   'csocast.mdb table Regulators for site-specific explanations.')
                            logging.warning('WARNING : %s', msg)

            message_prefix = '\nSystem Msg,'
            if not any(summ.LastHybridStatus > 0 for summ in summaries):
                hybrid_f.write(message_prefix + 'Sewer monitor network down. Please check back later.')
            else:
                with open(os.path.join(working_dir, pubmsg_fname), 'r') as message_f:
                    message = message_f.read()
                    if len(message) > 0:
                        hybrid_f.write(message_prefix + message)

        csocast_cursor.execute("SELECT Name, Manual_Flag, Good, Volume_2day, LastPoll, Lat, Long, PeakIntensity_2day FROM RainGages")
        rain_summaries = csocast_cursor.fetchall()
        with open(rain_outpath, 'w') as f:
            writer = csv.writer(f, lineterminator='\n')
            for summary in rain_summaries:
                if summary.Manual_Flag is True or summary.Good is False:
                    message = 'Rain Gauge is currently down for maintenance.'
                elif summary.Volume_2day > 0:
                    message = 'Rain reported in the last 48 hours for this area.'
                elif summary.Volume_2day == 0:
                    message = 'No rain reported in the last 48 hours for this area.'
                else:
                    message = 'No data.'

                if summary.Manual_Flag is True or summary.Good is False:
                    writer.writerow((summary.Name, message, summary.LastPoll.strftime('%m/%d/%Y %H:%M:%S'),
                                     summary.Lat, summary.Long, "down for maintenance",
                                     "down for maintenance"))
                else:
                    writer.writerow((summary.Name, message, summary.LastPoll.strftime('%m/%d/%Y %H:%M:%S'),
                                     summary.Lat, summary.Long, round(summary.Volume_2day, 2),
                                     round(summary.PeakIntensity_2day, 2)))
                # if upload:
                #     uploadOutfile(rain_outpath, '/castdata/')
                #     uploadOutfile(hybrid_outpath, '/castdata/')
                #     uploadOutfile(monitor_outpath, '/admin/csocast')
                #     uploadOutfile(model_outpath, '/admin/csocast')

        if backup:
            csocast_cursor.execute("SELECT MIN(LastPoll) FROM Outfalls")
            update_dtime = csocast_cursor.fetchone()[0]
            sys_update_dtime = datetime.now()

            csocast_cursor.execute("""
                SELECT Name, LastMonitorStatus, LastModelStatus, LastHybridStatus, LastPoll
                FROM Outfalls
            """)

            outfall_summaries = csocast_cursor.fetchall()

            # backup_path = os.path.join(working_dir, 'CSOCast_Archive.mdb')
            # backup_db = pyodbc.connect('Driver={Microsoft Access Driver (*.mdb)};DBQ=' + backup_path, autocommit=True)
            backup_path = os.path.join(working_dir, 'CSOCast_Archive.accdb')
            backup_db = pyodbc.connect('Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + backup_path,
                                       autocommit=True)
            backup_cursor = backup_db.cursor()

            for outfall in outfall_summaries:
                qry_str = """
                    INSERT INTO """ + outfall.Name + """
                    VALUES (?, ?, ?, ?)
                """
                backup_cursor.execute(qry_str, (outfall.LastMonitorStatus, outfall.LastPoll, update_dtime, 'outfall'))
                backup_cursor.execute(qry_str, (outfall.LastModelStatus, outfall.LastPoll, update_dtime, 'model'))
                backup_cursor.execute(qry_str, (outfall.LastHybridStatus, outfall.LastPoll, update_dtime, 'hybrid'))

            # Track the reason why regulator meters are either flagged as bad, uninstalled, or failed the quality filter
            csocast_cursor.execute("""
                SELECT Name, Outfall, LastMonitorStatus, LastModelStatus, LastPoll
                , BadDataExplanation, Good, BadMeterExplanation, Monitored
                FROM Regulators
                WHERE (((LastMonitorStatus)=0 Or (LastMonitorStatus)=2))
                """)
            reg_BadExplanation_list = csocast_cursor.fetchall()

            for reg_bad in reg_BadExplanation_list:
                backup_cursor.execute("""
                    INSERT INTO BadMeterExplanation_Tracking
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """
                                      , (reg_bad.Name, reg_bad.Outfall, reg_bad.LastMonitorStatus
                                         , reg_bad.LastModelStatus, reg_bad.LastPoll, reg_bad.BadDataExplanation,
                                         reg_bad.Good
                                         , reg_bad.BadMeterExplanation, reg_bad.Monitored, update_dtime,
                                         sys_update_dtime))

            backup_cursor.close()
            backup_db.close()

    telog_cursor.close()
    telog_db.close()

    csocast_cursor.close()
    csocast_db.close()


def run(working_dir=None, upload=True, run_model=True, backup=True):
    if not working_dir:
        if '__file__' in globals():
            working_dir = os.path.dirname(__file__)

        if not working_dir:
            import __main__
            if hasattr(__main__, '__file__'):
                working_dir = os.getcwd()
            else:
                while True:
                    working_dir = input('Path to working directory (leave blank to exit): ')
                    if len(working_dir) == 0:
                        return
                    elif os.path.isdir(working_dir):
                        break
                    else:
                        print('No such directory.')

    logging.basicConfig(filename=os.path.join(working_dir, 'csocast.log'),
                        format='%(asctime)-15s %(message)s', level=logging.INFO)
    logging.info(''.join(['START RUN', '-' * 50]))

    try:
        csocast(working_dir, upload=upload, run_model=run_model, backup=backup)
    except:
        hybrid_outpath = os.path.join(working_dir, 'output/hybrid_out.txt')
        with open(hybrid_outpath, 'w') as hybrid_f:
            message_prefix = 'System Msg,'
            hybrid_f.write(message_prefix + 'Sewer monitor network down. Please check back later.')
        logging.exception('ERROR %s', 'csocast failed to run.')
        raise


if __name__ == '__main__':
    run()
