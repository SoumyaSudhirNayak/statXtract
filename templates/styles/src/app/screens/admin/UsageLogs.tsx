import { useNavigate } from "react-router";
import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { useState } from "react";
import { Filter } from "lucide-react";
import { AnimatedBackground as Background } from "../../components/AnimatedBackground";

export function UsageLogs() {
  const navigate = useNavigate();
  const adminData = JSON.parse(sessionStorage.getItem("adminData") || '{"fullName":"Admin"}');
  
  const [filters, setFilters] = useState({
    dateFrom: "",
    dateTo: "",
    action: "",
    minRows: "",
    maxRows: "",
  });
  const [hoveredBox, setHoveredBox] = useState<string | null>(null);

  const logs = [
    { user: "Rahul Sharma", dataset: "Census 2021", rows: 15000, date: "2026-03-18", action: "Query" },
    { user: "Priya Patel", dataset: "Economic Data", rows: 8500, date: "2026-03-17", action: "Download" },
    { user: "Amit Kumar", dataset: "Health Stats", rows: 22000, date: "2026-03-17", action: "Query" },
    { user: "Sneha Reddy", dataset: "Education", rows: 12000, date: "2026-03-16", action: "Export" },
    { user: "Vikram Singh", dataset: "Census 2021", rows: 5000, date: "2026-03-16", action: "Query" },
    { user: "Anjali Mehta", dataset: "Economic Data", rows: 18000, date: "2026-03-15", action: "Download" },
  ];

  return (
    <div className="min-h-screen relative">
      <Background />
      
      <div className="relative z-10">
        <Header userName={adminData.fullName} userRole="admin" />
        <Navigation userRole="admin" />

        <div className="max-w-[1800px] mx-auto px-10 py-12">
          <div className="mb-10 flex justify-between items-center">
            <BackButton to="/admin/dashboard" />
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Usage Logs</h1>
            <div className="w-[80px]"></div>
          </div>

          {/* Filters Section */}
          <div 
            onMouseEnter={() => setHoveredBox('filters')}
            onMouseLeave={() => setHoveredBox(null)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 mb-12 transition-all duration-300 shadow-xl"
            style={{
              borderColor: hoveredBox === 'filters' ? '#2E5BBA' : '#D1D5DB',
              boxShadow: hoveredBox === 'filters' ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
            }}
          >
            <div className="flex items-center gap-4 mb-8 pb-6 border-b-2 border-[#E5E7EB]">
              <Filter className="w-8 h-8 text-[#2E5BBA]" />
              <h3 className="text-2xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 700 }}>Filter Logs</h3>
            </div>
            
            <div className="grid grid-cols-1 md:grid-cols-5 gap-8">
              <div>
                <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Date From</label>
                <input
                  type="date"
                  value={filters.dateFrom}
                  onChange={(e) => setFilters({ ...filters, dateFrom: e.target.value })}
                  onMouseEnter={() => setHoveredBox('date-from')}
                  onMouseLeave={() => setHoveredBox('filters')}
                  className="w-full px-4 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] text-lg bg-white transition-all duration-300"
                  style={{
                    borderColor: hoveredBox === 'date-from' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'date-from' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>
              <div>
                <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Date To</label>
                <input
                  type="date"
                  value={filters.dateTo}
                  onChange={(e) => setFilters({ ...filters, dateTo: e.target.value })}
                  onMouseEnter={() => setHoveredBox('date-to')}
                  onMouseLeave={() => setHoveredBox('filters')}
                  className="w-full px-4 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] text-lg bg-white transition-all duration-300"
                  style={{
                    borderColor: hoveredBox === 'date-to' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'date-to' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>
              <div>
                <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Action Type</label>
                <select
                  value={filters.action}
                  onChange={(e) => setFilters({ ...filters, action: e.target.value })}
                  onMouseEnter={() => setHoveredBox('action-type')}
                  onMouseLeave={() => setHoveredBox('filters')}
                  className="w-full px-4 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] text-lg bg-white transition-all duration-300"
                  style={{
                    borderColor: hoveredBox === 'action-type' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'action-type' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                >
                  <option value="">All Actions</option>
                  <option value="Query">Query</option>
                  <option value="Download">Download</option>
                  <option value="Export">Export</option>
                </select>
              </div>
              <div>
                <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Min Rows</label>
                <input
                  type="number"
                  placeholder="e.g. 1000"
                  value={filters.minRows}
                  onChange={(e) => setFilters({ ...filters, minRows: e.target.value })}
                  onMouseEnter={() => setHoveredBox('min-rows')}
                  onMouseLeave={() => setHoveredBox('filters')}
                  className="w-full px-4 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] text-lg bg-white transition-all duration-300"
                  style={{
                    borderColor: hoveredBox === 'min-rows' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'min-rows' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>
              <div>
                <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Max Rows</label>
                <input
                  type="number"
                  placeholder="e.g. 50000"
                  value={filters.maxRows}
                  onChange={(e) => setFilters({ ...filters, maxRows: e.target.value })}
                  onMouseEnter={() => setHoveredBox('max-rows')}
                  onMouseLeave={() => setHoveredBox('filters')}
                  className="w-full px-4 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] text-lg bg-white transition-all duration-300"
                  style={{
                    borderColor: hoveredBox === 'max-rows' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'max-rows' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>
            </div>

            <div className="flex gap-4 mt-10">
              <button className="lift-on-hover bg-[#2E5BBA] text-white px-10 py-4 rounded-xl transition-all duration-300 hover:bg-[#16324F] border-none cursor-pointer text-xl font-bold shadow-lg">
                Apply Filters
              </button>
              <button 
                onClick={() => setFilters({ dateFrom: "", dateTo: "", action: "", minRows: "", maxRows: "" })}
                className="lift-on-hover bg-white text-[#6B7280] px-10 py-4 rounded-xl border-2 border-[#D1D5DB] transition-all duration-300 hover:bg-[#F5F7FA] cursor-pointer text-xl font-bold"
              >
                Clear All
              </button>
            </div>
          </div>

          {/* Logs Table */}
          <div 
            onMouseEnter={() => setHoveredBox('logs')}
            onMouseLeave={() => setHoveredBox(null)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl overflow-hidden transition-all duration-300 shadow-xl"
            style={{
              borderColor: hoveredBox === 'logs' ? '#2E7D32' : '#D1D5DB',
              boxShadow: hoveredBox === 'logs' ? '0 15px 40px rgba(46, 125, 50, 0.2)' : 'none'
            }}
          >
            <table className="w-full border-collapse">
              <thead>
                <tr className="bg-[#1F3A5F] text-white">
                  <th className="px-8 py-6 text-left font-bold uppercase tracking-wider text-lg">User</th>
                  <th className="px-8 py-6 text-left font-bold uppercase tracking-wider text-lg">Dataset</th>
                  <th className="px-8 py-6 text-left font-bold uppercase tracking-wider text-lg">Action</th>
                  <th className="px-8 py-6 text-left font-bold uppercase tracking-wider text-lg">Rows Affected</th>
                  <th className="px-8 py-6 text-left font-bold uppercase tracking-wider text-lg">Date</th>
                </tr>
              </thead>
              <tbody>
                {logs.map((log, index) => (
                  <tr key={index} className="border-b border-[#E5E7EB] hover:bg-[#2E5BBA]/5 transition-colors">
                    <td className="px-8 py-6 font-bold text-xl text-[#1C1C1C]">{log.user}</td>
                    <td className="px-8 py-6 text-xl text-[#1C1C1C]">{log.dataset}</td>
                    <td className="px-8 py-6">
                      <span className={`px-5 py-2 rounded-full text-sm font-black uppercase tracking-wider ${
                        log.action === 'Query' ? 'bg-blue-100 text-blue-700 border-2 border-blue-200' :
                        log.action === 'Download' ? 'bg-green-100 text-green-700 border-2 border-green-200' :
                        'bg-orange-100 text-orange-700 border-2 border-orange-200'
                      }`}>
                        {log.action}
                      </span>
                    </td>
                    <td className="px-8 py-6 text-xl font-bold text-[#1C1C1C]">{log.rows.toLocaleString()}</td>
                    <td className="px-8 py-6 text-xl text-[#6B7280]">{log.date}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}