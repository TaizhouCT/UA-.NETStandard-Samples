/* ========================================================================
 * Copyright (c) 2005-2019 The OPC Foundation, Inc. All rights reserved.
 *
 * OPC Foundation MIT License 1.00
 * 
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * The complete license agreement can be found here:
 * http://opcfoundation.org/License/MIT/1.00/
 * ======================================================================*/

using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Xml;
using System.IO;
using System.Threading;
using System.Reflection;
using Opc.Ua;
using Opc.Ua.Server;
using System.Data;
using System.Data.OleDb;

namespace Quickstarts.EmptyServer
{
    /// <summary>
    /// A node manager for a server that exposes several variables.
    /// </summary>
    public class EmptyNodeManager : CustomNodeManager2
    {
        #region Constructors
        /// <summary>
        /// Initializes the node manager.
        /// </summary>
        public EmptyNodeManager(IServerInternal server, ApplicationConfiguration configuration)
        :
            base(server, configuration, Namespaces.Empty)
        {
            SystemContext.NodeIdFactory = this;

            // get the configuration for the node manager.
            m_configuration = configuration.ParseExtension<EmptyServerConfiguration>();

            // use suitable defaults if no configuration exists.
            if (m_configuration == null)
            {
                m_configuration = new EmptyServerConfiguration();
            }
        }
        #endregion
        
        #region IDisposable Members
        /// <summary>
        /// An overrideable version of the Dispose.
        /// </summary>
        protected override void Dispose(bool disposing)
        {  
            if (disposing)
            {
                // TBD
            }
        }
        #endregion

        #region INodeIdFactory Members
        /// <summary>
        /// Creates the NodeId for the specified node.
        /// </summary>
        public override NodeId New(ISystemContext context, NodeState node)
        {
            return node.NodeId;
        }
        #endregion

        private void InitConfig()
        {
            if (!System.IO.File.Exists(CONF_PATH))
                Environment.Exit(Environment.ExitCode);

            var config = new System.IO.FileInfo(CONF_PATH);
            log4net.Config.XmlConfigurator.Configure(config);

            try
            {
                XmlDocument xdoc = new XmlDocument();
                xdoc.Load(CONF_PATH);

                XmlNode node = xdoc.SelectSingleNode("/configuration/sqlserver");
                connStr = node.Attributes["connstr"].Value;
            }
            catch (Exception ex)
            {
                log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType)
                    .Error("error when parsing config file.", ex);
                Environment.Exit(Environment.ExitCode);
            }
        }

        private BaseObjectState AddObject(
            IDictionary<NodeId, IList<IReference>> externalReferences, uint idx, string name)
        {
            BaseObjectState equipment = new BaseObjectState(null);
            equipment.NodeId = new NodeId(idx, NamespaceIndex);
            equipment.BrowseName = new QualifiedName(name, NamespaceIndex);
            equipment.DisplayName = equipment.BrowseName.Name;
            equipment.TypeDefinitionId = ObjectTypeIds.BaseObjectType;
            IList<IReference> references = null;
            if (!externalReferences.TryGetValue(ObjectIds.ObjectsFolder, out references))
                externalReferences[ObjectIds.ObjectsFolder] = references = new List<IReference>();
            equipment.AddReference(ReferenceTypeIds.Organizes, true, ObjectIds.ObjectsFolder);
            references.Add(new NodeStateReference(ReferenceTypeIds.Organizes, false, equipment.NodeId));
            return equipment;
        }

        private PropertyState AddProperty(BaseObjectState baseObj,
            uint idx, string name, NodeId type, object value)
        {
            PropertyState property = new PropertyState(baseObj);
            property.NodeId = new NodeId(idx.ToString() + "-" + name, NamespaceIndex);
            property.BrowseName = new QualifiedName(name, NamespaceIndex);
            property.DisplayName = baseObj.BrowseName.Name + "-" + name;
            property.Description = "";
            property.TypeDefinitionId = VariableTypeIds.PropertyType;
            property.ReferenceTypeId = ReferenceTypeIds.HasProperty;
            property.DataType = type;
            property.Value = value;
            property.ValueRank = ValueRanks.Scalar;
            property.OnReadValue = OnReadRecord;
            baseObj.AddChild(property);
            return property;
        }

        private ServiceResult OnReadRecord(ISystemContext context, NodeState node,
            NumericRange indexRange, QualifiedName dataEncoding,
            ref object value, ref StatusCode statusCode, ref DateTime timestamp)
        {
            uint parentId = (uint)((PropertyState)node).Parent.NodeId.Identifier;
            if (!equipments.ContainsKey(parentId))
                return ServiceResult.Good;

            value = equipments[parentId][node.BrowseName.Name];
            timestamp = DateTime.Now;
            return ServiceResult.Good;

        }

        private void ReadDb()
        {
            try
            {
                while (true)
                {
                    // Query Record
                    {
                        OleDbConnection conn = new OleDbConnection(connStr);
                        conn.Open();
                        string sql = String.Format(
                            "SELECT TOP 1000 * FROM tblRecord ORDER BY ID DESC");
                        OleDbDataAdapter adapter = new OleDbDataAdapter(sql, conn);
                        DataSet ds = new DataSet();
                        adapter.Fill(ds);
                        conn.Close();
                        DataTableReader dr = ds.CreateDataReader();
                        while (dr.Read())
                        {
                            uint id = uint.Parse(dr["EquipmentID"].ToString());
                            equipments[id]["Value"] = dr["clValue"];
                            equipments[id]["TimeStamp"] = dr["clTime"];
                        }
                    }

                    // Query Abnormity 
                    {
                        OleDbConnection conn = new OleDbConnection(connStr);
                        conn.Open();
                        string sql = String.Format(
                            "SELECT TOP 1000 * FROM tblAbnormity ORDER BY ID DESC");
                        OleDbDataAdapter adapter = new OleDbDataAdapter(sql, conn);
                        DataSet ds = new DataSet();
                        adapter.Fill(ds);
                        conn.Close();
                        DataTableReader dr = ds.CreateDataReader();
                        while (dr.Read())
                        {
                            uint id = uint.Parse(dr["EquipmentID"].ToString());
                            equipments[id]["AbnormityStatus"] = dr["Status"];
                            equipments[id]["AbnormityValue"] = dr["MaxValue"];
                        }
                    }

                    // Query Equipment
                    {
                        OleDbConnection conn = new OleDbConnection(connStr);
                        conn.Open();
                        string sql = String.Format("SELECT * FROM tblEquipment");
                        OleDbDataAdapter adapter = new OleDbDataAdapter(sql, conn);
                        DataSet ds = new DataSet();
                        adapter.Fill(ds);
                        conn.Close();
                        DataTableReader dr = ds.CreateDataReader();
                        while (dr.Read())
                        {
                            uint id = uint.Parse(dr["ID"].ToString());
                            equipments[id]["Address"] = dr["Address"];
                            equipments[id]["MinValue"] = dr["MinValue"];
                            equipments[id]["MaxValue"] = dr["MaxValue"];
                            equipments[id]["UpperLimit"] = dr["UpperLimit"];
                            equipments[id]["LowerLimit"] = dr["LowerLimit"];
                            equipments[id]["State"] = dr["State"];
                        }
                    }

                    System.Threading.Thread.Sleep(20 * 1000);
                }
            }
            catch (Exception ex)
            {
                log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType)
                    .Error("ReadDB()", ex);
            }
        }

        private void tianyu_init(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            try
            {
                OleDbConnection conn = new OleDbConnection(connStr);
                conn.Open();
                OleDbDataAdapter adapter = new OleDbDataAdapter("SELECT * FROM tblEquipment", conn);
                DataSet ds = new DataSet();
                adapter.Fill(ds);
                conn.Close();

                DataTableReader dr = ds.CreateDataReader();
                while (dr.Read())
                {
                    uint idx = uint.Parse(dr["ID"].ToString());

                    BaseObjectState baseObj = AddObject(externalReferences, idx, dr["Name"].ToString());
                    AddProperty(baseObj, idx, "ID", DataTypeIds.UInt32, dr["ID"]);
                    AddProperty(baseObj, idx, "Name", DataTypeIds.String, dr["Name"]);
                    AddProperty(baseObj, idx, "Address", DataTypeIds.String, dr["Address"]);
                    AddProperty(baseObj, idx, "MinValue", DataTypeIds.Double, dr["MinValue"]);
                    AddProperty(baseObj, idx, "MaxValue", DataTypeIds.Double, dr["MaxValue"]);
                    AddProperty(baseObj, idx, "UpperLimit", DataTypeIds.Double, dr["UpperLimit"]);
                    AddProperty(baseObj, idx, "LowerLimit", DataTypeIds.Double, dr["LowerLimit"]);
                    AddProperty(baseObj, idx, "State", DataTypeIds.Int32, dr["State"]);
                    AddProperty(baseObj, idx, "Value", DataTypeIds.Double, 0);
                    AddProperty(baseObj, idx, "TimeStamp", DataTypeIds.DateTime, new DateTime(1, 1, 1));
                    AddProperty(baseObj, idx, "AbnormityStatus", DataTypeIds.String, "");
                    AddProperty(baseObj, idx, "AbnormityValue", DataTypeIds.Double, 0);
                    AddPredefinedNode(SystemContext, baseObj);

                    Dictionary<string, object> equipment = new Dictionary<string, object>();
                    equipment.Add("ID", dr["ID"]);
                    equipment.Add("Name", dr["Name"]);
                    equipment.Add("Address", dr["Address"]);
                    equipment.Add("MinValue", dr["MinValue"]);
                    equipment.Add("MaxValue", dr["MaxValue"]);
                    equipment.Add("UpperLimit", dr["UpperLimit"]);
                    equipment.Add("LowerLimit", dr["LowerLimit"]);
                    equipment.Add("State", dr["State"]);
                    equipment.Add("Value", 0);
                    equipment.Add("TimeStamp", new DateTime(1, 1, 1));
                    equipment.Add("AbnormityStatus", "");
                    equipment.Add("AbnormityValue", 0);
                    equipments.Add(idx, equipment);
                }

                equipments_thread = new Thread(() => { ReadDb(); });
                equipments_thread.Start();
            }
            catch (Exception ex)
            {
                log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType)
                    .Error("tianyu_init()", ex);
            }
        }

        #region INodeManager Members
        /// <summary>
        /// Does any initialization required before the address space can be used.
        /// </summary>
        /// <remarks>
        /// The externalReferences is an out parameter that allows the node manager to link to nodes
        /// in other node managers. For example, the 'Objects' node is managed by the CoreNodeManager and
        /// should have a reference to the root folder node(s) exposed by this node manager.  
        /// </remarks>
        public override void CreateAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            lock (Lock)
            {
                //BaseObjectState trigger = new BaseObjectState(null);

                //trigger.NodeId = new NodeId(1, NamespaceIndex);
                //trigger.BrowseName = new QualifiedName("Trigger", NamespaceIndex);
                //trigger.DisplayName = trigger.BrowseName.Name;
                //trigger.TypeDefinitionId = ObjectTypeIds.BaseObjectType; 

                //// ensure trigger can be found via the server object. 
                //IList<IReference> references = null;

                //if (!externalReferences.TryGetValue(ObjectIds.ObjectsFolder, out references))
                //{
                //    externalReferences[ObjectIds.ObjectsFolder] = references = new List<IReference>();
                //}

                //trigger.AddReference(ReferenceTypeIds.Organizes, true, ObjectIds.ObjectsFolder);
                //references.Add(new NodeStateReference(ReferenceTypeIds.Organizes, false, trigger.NodeId));

                //PropertyState property = new PropertyState(trigger);

                //property.NodeId = new NodeId(2, NamespaceIndex);
                //property.BrowseName = new QualifiedName("Matrix", NamespaceIndex);
                //property.DisplayName = property.BrowseName.Name;
                //property.TypeDefinitionId = VariableTypeIds.PropertyType;
                //property.ReferenceTypeId = ReferenceTypeIds.HasProperty;
                //property.DataType = DataTypeIds.Int32;
                //property.ValueRank = ValueRanks.TwoDimensions;
                //property.ArrayDimensions = new ReadOnlyList<uint>(new uint[] { 2, 2 });

                //trigger.AddChild(property);

                //// save in dictionary. 
                //AddPredefinedNode(SystemContext, trigger);

                //ReferenceTypeState referenceType = new ReferenceTypeState();

                //referenceType.NodeId = new NodeId(3, NamespaceIndex);
                //referenceType.BrowseName = new QualifiedName("IsTriggerSource", NamespaceIndex);
                //referenceType.DisplayName = referenceType.BrowseName.Name;
                //referenceType.InverseName = new LocalizedText("IsSourceOfTrigger");
                //referenceType.SuperTypeId = ReferenceTypeIds.NonHierarchicalReferences;

                //if (!externalReferences.TryGetValue(ObjectIds.Server, out references))
                //{
                //    externalReferences[ObjectIds.Server] = references = new List<IReference>();
                //}

                //trigger.AddReference(referenceType.NodeId, false, ObjectIds.Server);
                //references.Add(new NodeStateReference(referenceType.NodeId, true, trigger.NodeId));

                //// save in dictionary. 
                //AddPredefinedNode(SystemContext, referenceType);

                InitConfig();
                tianyu_init(externalReferences);
            } 
        }

        /// <summary>
        /// Frees any resources allocated for the address space.
        /// </summary>
        public override void DeleteAddressSpace()
        {
            lock (Lock)
            {
                // TBD
                equipments_thread.Abort();
            }
        }

        /// <summary>
        /// Returns a unique handle for the node.
        /// </summary>
        protected override NodeHandle GetManagerHandle(ServerSystemContext context, NodeId nodeId, IDictionary<NodeId, NodeState> cache)
        {
            lock (Lock)
            {
                // quickly exclude nodes that are not in the namespace. 
                if (!IsNodeIdInNamespace(nodeId))
                {
                    return null;
                }

                NodeState node = null;

                if (!PredefinedNodes.TryGetValue(nodeId, out node))
                {
                    return null;
                }

                NodeHandle handle = new NodeHandle();

                handle.NodeId = nodeId;
                handle.Node = node;
                handle.Validated = true;

                return handle;
            } 
        }

        /// <summary>
        /// Verifies that the specified node exists.
        /// </summary>
        protected override NodeState ValidateNode(
            ServerSystemContext context,
            NodeHandle handle,
            IDictionary<NodeId, NodeState> cache)
        {
            // not valid if no root.
            if (handle == null)
            {
                return null;
            }

            // check if previously validated.
            if (handle.Validated)
            {
                return handle.Node;
            }
            
            // TBD

            return null;
        }
        #endregion

        #region Overridden Methods
        #endregion

        #region Private Fields
        private EmptyServerConfiguration m_configuration;
        private static readonly string CONF_PATH = AppDomain.CurrentDomain.BaseDirectory + "tianyu_opc.xml";
        private String connStr = @"Provider=SQLOLEDB;Data Source = 127.0.0.1;User ID = sa;Password=123456;Initial Catalog = ehs";
        private Dictionary<uint, Dictionary<string, object>> equipments =
            new Dictionary<uint, Dictionary<string, object>>();
        private object equipments_lock;
        private Thread equipments_thread;
        #endregion
    }
}
