import { useState } from "react";
import { useNavigate } from "react-router";
import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { X } from "lucide-react";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function ViewSchema() {
  const navigate = useNavigate();
  const adminData = JSON.parse(sessionStorage.getItem("adminData") || '{"fullName":"Admin"}');
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [hoveredBox, setHoveredBox] = useState<string | null>(null);

  const schemas = [
    { name: "census_2021", tables: ["states", "districts", "cities"] },
    { name: "economic_data", tables: ["gdp", "employment", "trade"] },
  ];

  const tableDetails = {
    states: {
      columns: ["id", "name", "population", "area", "capital"],
      rowCount: 28,
    },
    gdp: {
      columns: ["year", "state_id", "gdp_value", "growth_rate"],
      rowCount: 560,
    },
  };

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <Header userName={adminData.fullName} userRole="admin" />
        <Navigation userRole="admin" />

        <div className="max-w-[1600px] mx-auto px-10 py-12">
          <div className="mb-10 flex justify-between items-center">
            <BackButton to="/admin/dashboard" />
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>View Schema & Tables</h1>
            <div className="w-[80px]"></div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-4 gap-12">
            <div 
              onMouseEnter={() => setHoveredBox('schemas')}
              onMouseLeave={() => setHoveredBox(null)}
              className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 transition-all duration-300 shadow-xl"
              style={{
                borderColor: hoveredBox === 'schemas' ? '#2E5BBA' : '#D1D5DB',
                boxShadow: hoveredBox === 'schemas' ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
              }}
            >
              <h3 className="text-3xl text-[#1C1C1C] mb-8 font-bold" style={{ fontFamily: 'var(--font-head)' }}>Schemas</h3>
              {schemas.map((schema) => (
                <div key={schema.name} className="mb-10 last:mb-0">
                  <h4 className="text-[#2E5BBA] mb-4 text-sm uppercase tracking-widest font-black border-b-2 border-[#2E5BBA]/20 pb-2">{schema.name}</h4>
                  <div className="space-y-3">
                    {schema.tables.map((table) => (
                      <div
                        key={table}
                        onClick={() => setSelectedTable(table)}
                        onMouseEnter={() => setHoveredBox(`table-${table}`)}
                        onMouseLeave={() => setHoveredBox('schemas')}
                        className={`px-5 py-3 rounded-xl cursor-pointer transition-all border-2 text-lg font-bold ${
                          selectedTable === table 
                            ? "bg-[#2E5BBA] text-white border-[#2E5BBA] shadow-lg scale-[1.02]" 
                            : "bg-[#F5F7FA] text-[#1C1C1C] border-transparent hover:bg-white hover:border-[#2E5BBA]/30 hover:shadow-md"
                        }`}
                        style={{
                          borderColor: selectedTable === table ? '#F4A300' : (hoveredBox === `table-${table}` ? '#2E5BBA' : 'transparent'),
                        }}
                      >
                        {table}
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>

            <div className="md:col-span-3">
              {selectedTable ? (
                <div 
                  onMouseEnter={() => setHoveredBox('table-details')}
                  onMouseLeave={() => setHoveredBox(null)}
                  className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-12 transition-all duration-300 shadow-xl h-full"
                  style={{
                    borderColor: hoveredBox === 'table-details' ? '#2E7D32' : '#D1D5DB',
                    boxShadow: hoveredBox === 'table-details' ? '0 15px 40px rgba(46, 125, 50, 0.2)' : 'none'
                  }}
                >
                  <div className="flex justify-between items-center mb-10 pb-6 border-b-4 border-[#E5E7EB]">
                    <h3 className="text-3xl text-[#1C1C1C] m-0 font-bold" style={{ fontFamily: 'var(--font-head)' }}>
                      Table: <span className="text-[#2E7D32]">{selectedTable}</span>
                    </h3>
                    <button
                      onClick={() => setSelectedTable(null)}
                      className="bg-[#F5F7FA] border-2 border-[#D1D5DB] rounded-full p-2 cursor-pointer hover:bg-[#ffe5e5] hover:border-[#d4183d] transition-all"
                    >
                      <X className="w-8 h-8 text-[#6B7280] hover:text-[#d4183d]" />
                    </button>
                  </div>
                  <div className="space-y-12">
                    <div>
                      <p className="text-[#6B7280] text-sm mb-6 font-black uppercase tracking-widest">Columns:</p>
                      <div className="flex flex-wrap gap-4">
                        {(tableDetails[selectedTable as keyof typeof tableDetails]?.columns || []).map((col) => (
                          <span
                            key={col}
                            className="px-6 py-3 bg-white border-2 border-[#D1D5DB] rounded-xl text-xl font-bold text-[#1C1C1C] shadow-sm hover:border-[#2E5BBA] hover:shadow-md transition-all duration-300"
                          >
                            {col}
                          </span>
                        ))}
                      </div>
                    </div>
                    <div className="bg-[#F5F7FA] p-8 rounded-xl border-2 border-[#D1D5DB]">
                      <p className="text-2xl text-[#1C1C1C] m-0 font-bold">
                        <span className="text-[#6B7280] mr-4 uppercase text-sm tracking-widest font-black">Total Rows:</span> 
                        {tableDetails[selectedTable as keyof typeof tableDetails]?.rowCount || 0}
                      </p>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="h-full flex items-center justify-center bg-white/50 backdrop-blur-sm border-2 border-dashed border-[#D1D5DB] rounded-xl p-24 text-center shadow-inner">
                  <p className="text-[#6B7280] text-3xl font-bold leading-relaxed">Select a table from the sidebar to view its schema details</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}