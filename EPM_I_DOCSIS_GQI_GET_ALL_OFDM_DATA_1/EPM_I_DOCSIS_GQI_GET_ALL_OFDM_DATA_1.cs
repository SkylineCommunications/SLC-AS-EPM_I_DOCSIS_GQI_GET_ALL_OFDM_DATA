/*
****************************************************************************
*  Copyright (c) 2023,  Skyline Communications NV  All Rights Reserved.    *
****************************************************************************

By using this script, you expressly agree with the usage terms and
conditions set out below.
This script and all related materials are protected by copyrights and
other intellectual property rights that exclusively belong
to Skyline Communications.

A user license granted for this script is strictly for personal use only.
This script may not be used in any way by anyone without the prior
written consent of Skyline Communications. Any sublicensing of this
script is forbidden.

Any modifications to this script by the user are only allowed for
personal use and within the intended purpose of the script,
and will remain the sole responsibility of the user.
Skyline Communications will not be responsible for any damages or
malfunctions whatsoever of the script resulting from a modification
or adaptation by the user.

The content of this script is confidential information.
The user hereby agrees to keep this confidential information strictly
secret and confidential and not to disclose or reveal it, in whole
or in part, directly or indirectly to any person, entity, organization
or administration without the prior written consent of
Skyline Communications.

Any inquiries can be addressed to:

	Skyline Communications NV
	Ambachtenstraat 33
	B-8870 Izegem
	Belgium
	Tel.	: +32 51 31 35 69
	Fax.	: +32 51 31 01 29
	E-mail	: info@skyline.be
	Web		: www.skyline.be
	Contact	: Ben Vandenberghe

****************************************************************************
Revision History:

DATE		VERSION		AUTHOR			COMMENTS

dd/mm/2023	1.0.0.1		XXX, Skyline	Initial version
****************************************************************************
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net.Messages;
using SLDataGateway.API.Types.Results.Paging;

[GQIMetaData(Name = "All OFDM Channels Data")]
public class CmData : IGQIDataSource, IGQIInputArguments, IGQIOnInit
{
    private readonly GQIStringArgument frontEndElementArg = new GQIStringArgument("FE Element")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument filterEntityArg = new GQIStringArgument("Filter Entity")
    {
        IsRequired = false,
    };

    private readonly GQIStringArgument entityBeTablePidArg = new GQIStringArgument("BE Entity Table PID")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument entityBeCcapIdsArg = new GQIStringArgument("Entity CCAP Dma/Eid IDX")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument entityBeCmCollectorIdsArg = new GQIStringArgument("Entity Collector Dma/Eid IDX")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument entityNameCcapPidArg = new GQIStringArgument("Entity Name CCAP PID")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument entityNameCmCollectorPidArg = new GQIStringArgument("Entity Name Collector PID")
    {
        IsRequired = true,
    };

    private GQIDMS _dms;

    private string frontEndElement = String.Empty;

    private string filterEntity = String.Empty;

    private int entityBeTablePid = 0;

    private int entityBeCcapIdx = 0;

    private int entityBeCmCollectorIdx = 0;

    private int entityNameCcapPid = 0;

    private int entityNameCmCollectorPid = 0;

    private List<GQIRow> listGqiRows = new List<GQIRow> { };

    public OnInitOutputArgs OnInit(OnInitInputArgs args)
    {
        _dms = args.DMS;
        return new OnInitOutputArgs();
    }

    public GQIArgument[] GetInputArguments()
    {
        return new GQIArgument[]
        {
            frontEndElementArg,
            filterEntityArg,
            entityBeTablePidArg,
            entityBeCcapIdsArg,
            entityBeCmCollectorIdsArg,
            entityNameCcapPidArg,
            entityNameCmCollectorPidArg,
        };
    }

    public GQIColumn[] GetColumns()
    {
        return new GQIColumn[]
        {
            new GQIStringColumn("Interface Name"),
            new GQIIntColumn("Channel ID"),
            new GQIDoubleColumn("Utilization"),
            new GQIDoubleColumn("Average RX Power"),
            new GQIDoubleColumn("Lower Frequency"),
            new GQIDoubleColumn("Upper Frequency"),
            new GQIStringColumn("Service Group Name"),
            new GQIStringColumn("Node Segment Name"),
            new GQIStringColumn("DS Port Name"),
        };
    }

    public GQIPage GetNextPage(GetNextPageInputArgs args)
    {
        return new GQIPage(listGqiRows.ToArray())
        {
            HasNextPage = false,
        };
    }

    public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
    {
        listGqiRows.Clear();
        try
        {
            frontEndElement = args.GetArgumentValue(frontEndElementArg);
            filterEntity = args.GetArgumentValue(filterEntityArg);
            entityBeTablePid = Convert.ToInt32(args.GetArgumentValue(entityBeTablePidArg));
            entityBeCcapIdx = Convert.ToInt32(args.GetArgumentValue(entityBeCcapIdsArg));
            entityBeCmCollectorIdx = Convert.ToInt32(args.GetArgumentValue(entityBeCmCollectorIdsArg));
            entityNameCcapPid = Convert.ToInt32(args.GetArgumentValue(entityNameCcapPidArg));
            entityNameCmCollectorPid = Convert.ToInt32(args.GetArgumentValue(entityNameCmCollectorPidArg));

            var backEndHelper = GetBackEndElement();
            if (backEndHelper == null)
            {
                return new OnArgumentsProcessedOutputArgs();
            }

            var ccapTable = GetTable(Convert.ToString(backEndHelper.CcapId), 5700, new List<string>
            {
                String.Format("forceFullTable=true;fullFilter=({0}=={1})", entityNameCcapPid, filterEntity),
            });

            var collectorTable = GetTable(Convert.ToString(backEndHelper.CollectorId), 2200, new List<string>
            {
                String.Format("forceFullTable=true;fullFilter=({0}=={1})", entityNameCmCollectorPid, filterEntity),
            });

            Dictionary<string, CcapOfdmOverview> ccapRows = ExtractCcapData(ccapTable);

            Dictionary<string, CmCollectorOfdmOverview> collectorRows = ExtractCollectorData(collectorTable);

            CreateOfdmRows(ccapRows, collectorRows);
        }
        catch
        {
            listGqiRows = new List<GQIRow>();
        }

        return new OnArgumentsProcessedOutputArgs();
    }

    public List<HelperPartialSettings[]> GetTable(string element, int tableId, List<string> filter)
    {
        var columns = new List<HelperPartialSettings[]>();

        var elementIds = element.Split('/');
        if (elementIds.Length > 1 && Int32.TryParse(elementIds[0], out int dmaId) && Int32.TryParse(elementIds[1], out int elemId))
        {
            // Retrieve client connections from the DMS using a GetInfoMessage request
            var getPartialTableMessage = new GetPartialTableMessage(dmaId, elemId, tableId, filter.ToArray());
            var paramChange = (ParameterChangeEventMessage)_dms.SendMessage(getPartialTableMessage);

            if (paramChange != null && paramChange.NewValue != null && paramChange.NewValue.ArrayValue != null)
            {
                columns = paramChange.NewValue.ArrayValue
                    .Where(av => av != null && av.ArrayValue != null)
                    .Select(p => p.ArrayValue.Where(v => v != null)
                    .Select(c => new HelperPartialSettings
                    {
                        CellValue = c.CellValue.InteropValue,
                        DisplayValue = c.CellValue.CellDisplayValue,
                        DisplayType = c.CellDisplayState,
                    }).ToArray()).ToList();
            }
        }

        return columns;
    }

    public BackEndHelper GetBackEndElement()
    {
        if (String.IsNullOrEmpty(filterEntity))
        {
            return null;
        }

        var backendTable = GetTable(frontEndElement, 1200500, new List<string>
        {
            "forceFullTable=true",
        });

        if (backendTable != null && backendTable.Any())
        {
            for (int i = 0; i < backendTable[0].Count(); i++)
            {
                var key = Convert.ToString(backendTable[0][i].CellValue);

                var backendEntityTable = GetTable(key, entityBeTablePid, new List<string>
                {
                    String.Format("forceFullTable=true;fullFilter=({0}=={1})", entityBeTablePid + 2, filterEntity),
                });

                if (backendEntityTable != null && backendEntityTable.Any() && backendEntityTable[0].Length > 0)
                {
                    return new BackEndHelper
                    {
                        ElementId = key,
                        CcapId = Convert.ToString(backendEntityTable[entityBeCcapIdx][0].CellValue),
                        CollectorId = Convert.ToString(backendEntityTable[entityBeCmCollectorIdx][0].CellValue),
                        EntityId = Convert.ToString(backendEntityTable[0][0].CellValue),
                    };
                }
            }
        }

        return null;
    }

    public string ParseDoubleValue(double doubleValue, string unit)
    {
        if (doubleValue.Equals(-1))
        {
            return "N/A";
        }

        return Math.Round(doubleValue, 2) + " " + unit;
    }

    public string ParseStringValue(string stringValue)
    {
        if (String.IsNullOrEmpty(stringValue) || stringValue == "-1")
        {
            return "N/A";
        }

        return stringValue;
    }

    private static Dictionary<string, CcapOfdmOverview> ExtractCcapData(List<HelperPartialSettings[]> ccapTable)
    {
        Dictionary<string, CcapOfdmOverview> ccapRows = new Dictionary<string, CcapOfdmOverview>();
        if (ccapTable != null && ccapTable.Any())
        {
            for (int i = 0; i < ccapTable[0].Count(); i++)
            {
                var key = Convert.ToString(ccapTable[0][i].CellValue);
                var ccapRow = new CcapOfdmOverview
                {
                    OfdmaId = key,
                    OfdmInterfaceName = Convert.ToString(ccapTable[2][i].CellValue),
                    OfdmChannelId = Convert.ToInt32(ccapTable[3][i].CellValue),
                    OfdmUtilization = Convert.ToDouble(ccapTable[4][i].CellValue),
                    OfdmLowerFrequency = Convert.ToDouble(ccapTable[11][i].CellValue),
                    OfdmUpperFrequency = Convert.ToDouble(ccapTable[12][i].CellValue),
                    OfdmNodeSegmentName = Convert.ToString(ccapTable[7][i].CellValue),
                    OfdmServiceGroupName = Convert.ToString(ccapTable[5][i].CellValue),
                    OfdmDsPortName = Convert.ToString(ccapTable[9][i].CellValue),
                };

                ccapRows[key] = ccapRow;
            }
        }

        return ccapRows;
    }

    private static Dictionary<string, CmCollectorOfdmOverview> ExtractCollectorData(List<HelperPartialSettings[]> collectorTable)
    {
        Dictionary<string, CmCollectorOfdmOverview> collectorRows = new Dictionary<string, CmCollectorOfdmOverview>();
        if (collectorTable != null && collectorTable.Any())
        {
            for (int i = 0; i < collectorTable[0].Count(); i++)
            {
                var key = Convert.ToString(collectorTable[0][i].CellValue);
                var ccapRow = new CmCollectorOfdmOverview
                {
                    OfdmaId = key,
                    OfdmInterfaceName = Convert.ToString(collectorTable[2][i].CellValue),
                    OfdmRxPower = Convert.ToDouble(collectorTable[10][i].CellValue),
                };

                collectorRows[key] = ccapRow;
            }
        }

        return collectorRows;
    }

    private void CreateOfdmRows(Dictionary<string, CcapOfdmOverview> ccapRows, Dictionary<string, CmCollectorOfdmOverview> collectorRows)
    {
        foreach (var ccapRow in ccapRows)
        {
            var collectorOfdmData = collectorRows.ContainsKey(ccapRow.Key) ? collectorRows[ccapRow.Key] : new CmCollectorOfdmOverview { OfdmRxPower = -1 };
            List<GQICell> listGqiCells = new List<GQICell>
                {
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmInterfaceName,
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmChannelId,
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmUtilization,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmUtilization, "%"),
                    },
                    new GQICell
                    {
                        Value = collectorOfdmData.OfdmRxPower,
                        DisplayValue = ParseDoubleValue(collectorOfdmData.OfdmRxPower, "dBmV"),
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmLowerFrequency,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmLowerFrequency, "MHz"),
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmUpperFrequency,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmUpperFrequency, "MHz"),
                    },
                    new GQICell
                    {
                        Value = ParseStringValue(ccapRow.Value.OfdmServiceGroupName),
                    },
                    new GQICell
                    {
                        Value = ParseStringValue(ccapRow.Value.OfdmNodeSegmentName),
                    },
                    new GQICell
                    {
                        Value = ParseStringValue(ccapRow.Value.OfdmDsPortName),
                    },
                };

            var gqiRow = new GQIRow(listGqiCells.ToArray());

            listGqiRows.Add(gqiRow);
        }
    }
}

public class BackEndHelper
{
    public string ElementId { get; set; }

    public string CcapId { get; set; }

    public string CollectorId { get; set; }

    public string EntityId { get; set; }
}

public class HelperPartialSettings
{
    public object CellValue { get; set; }

    public object DisplayValue { get; set; }

    public ParameterDisplayType DisplayType { get; set; }
}

public class CcapOfdmOverview
{
    public string OfdmaId { get; set; }

    public string OfdmInterfaceName { get; set; }

    public int OfdmChannelId { get; set; }

    public double OfdmUtilization { get; set; }

    public double OfdmLowerFrequency { get; set; }

    public double OfdmUpperFrequency { get; set; }

    public string OfdmServiceGroupName { get; set; }

    public string OfdmNodeSegmentName { get; set; }

    public string OfdmDsPortName { get; set; }
}

public class CmCollectorOfdmOverview
{
    public string OfdmaId { get; set; }

    public string OfdmInterfaceName { get; set; }

    public double OfdmRxPower { get; set; }
}