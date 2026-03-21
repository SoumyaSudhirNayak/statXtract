import { useNavigate } from "react-router";
import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { useState } from "react";
import { PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function UserManagement() {
  const navigate = useNavigate();
  const adminData = JSON.parse(sessionStorage.getItem("adminData") || '{"fullName":"Admin"}');
  const [hoveredBox, setHoveredBox] = useState<string | null>(null);

  const users = [
    { name: "Rahul Sharma", type: "Student", usage: "15 GB", lastActive: "2026-03-18" },
    { name: "Priya Patel", type: "Researcher", usage: "28 GB", lastActive: "2026-03-17" },
    { name: "Amit Kumar", type: "Analyst", usage: "42 GB", lastActive: "2026-03-17" },
    { name: "Sneha Reddy", type: "Private", usage: "19 GB", lastActive: "2026-03-16" },
  ];

  const userTypes = [
    { name: "Student", value: 120 },
    { name: "Researcher", value: 85 },
    { name: "Private", value: 45 },
    { name: "Analyst", value: 32 },
  ];

  const usageTrends = [
    { month: "Jan", queries: 450 },
    { month: "Feb", queries: 520 },
    { month: "Mar", queries: 380 },
  ];

  const COLORS = ["#2E5BBA", "#2E7D32", "#F4A300", "#4A90E2"];

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <Header userName={adminData.fullName} userRole="admin" />
        <Navigation userRole="admin" />

        <div className="max-w-[1800px] mx-auto px-10 py-12">
          <div className="mb-10 flex justify-between items-center">
            <BackButton to="/admin/dashboard" />
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>User Management</h1>
            <div className="w-[80px]"></div>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-4 gap-12">
            {/* Sidebar */}
            <div 
              onMouseEnter={() => setHoveredBox('sidebar')}
              onMouseLeave={() => setHoveredBox(null)}
              className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 transition-all duration-300 shadow-xl"
              style={{
                borderColor: hoveredBox === 'sidebar' ? '#2E5BBA' : '#D1D5DB',
                boxShadow: hoveredBox === 'sidebar' ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
              }}
            >
              <h3 className="text-3xl text-[#1C1C1C] mb-8 font-bold" style={{ fontFamily: 'var(--font-head)' }}>User Statistics</h3>
              <div className="space-y-6">
                <div className="flex justify-between items-center pb-5 border-b-4 border-[#2E5BBA]">
                  <span className="text-xl text-[#1C1C1C] font-bold">Total Users</span>
                  <span className="text-[#2E5BBA] font-black text-4xl">282</span>
                </div>
                {userTypes.map((type) => (
                  <div key={type.name} className="flex justify-between items-center px-4 py-3 hover:bg-[#F5F7FA] rounded-xl transition-colors border-b-2 border-transparent hover:border-[#D1D5DB]">
                    <span className="text-[#6B7280] text-xl font-bold">{type.name}</span>
                    <span className="text-[#1C1C1C] text-2xl font-black">{type.value}</span>
                  </div>
                ))}
              </div>
            </div>

            {/* Main Content */}
            <div className="lg:col-span-3 space-y-12">
              {/* Users Table */}
              <div 
                onMouseEnter={() => setHoveredBox('table')}
                onMouseLeave={() => setHoveredBox(null)}
                className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl overflow-hidden transition-all duration-300 shadow-xl"
                style={{
                  borderColor: hoveredBox === 'table' ? '#2E7D32' : '#D1D5DB',
                  boxShadow: hoveredBox === 'table' ? '0 15px 40px rgba(46, 125, 50, 0.2)' : 'none'
                }}
              >
                <div className="px-10 py-6 bg-[#1F3A5F] border-b-2 border-[#2E5BBA]">
                  <h3 className="text-2xl text-white m-0 font-bold" style={{ fontFamily: 'var(--font-head)' }}>Active Users</h3>
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full border-collapse">
                    <thead>
                      <tr className="bg-[#F5F7FA]">
                        <th className="px-8 py-6 text-left text-[#1C1C1C] font-black uppercase tracking-wider text-lg">Name</th>
                        <th className="px-8 py-6 text-left text-[#1C1C1C] font-black uppercase tracking-wider text-lg">Type</th>
                        <th className="px-8 py-6 text-left text-[#1C1C1C] font-black uppercase tracking-wider text-lg">Data Usage</th>
                        <th className="px-8 py-6 text-left text-[#1C1C1C] font-black uppercase tracking-wider text-lg">Last Active</th>
                      </tr>
                    </thead>
                    <tbody>
                      {users.map((user, index) => (
                        <tr key={index} className="border-t border-[#E5E7EB] hover:bg-[#2E5BBA]/5 transition-colors">
                          <td className="px-8 py-6 text-[#1C1C1C] font-bold text-xl">{user.name}</td>
                          <td className="px-8 py-6">
                            <span className="px-5 py-2 bg-blue-100 text-[#2E5BBA] border-2 border-blue-200 rounded-full text-sm font-black uppercase tracking-widest">
                              {user.type}
                            </span>
                          </td>
                          <td className="px-8 py-6 text-[#1C1C1C] font-bold text-xl">{user.usage}</td>
                          <td className="px-8 py-6 text-xl text-[#6B7280] font-medium">{user.lastActive}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Analytics */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-12">
                <div 
                  onMouseEnter={() => setHoveredBox('analytics-1')}
                  onMouseLeave={() => setHoveredBox(null)}
                  className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 transition-all duration-300 shadow-xl"
                  style={{
                    borderColor: hoveredBox === 'analytics-1' ? '#F4A300' : '#D1D5DB',
                    boxShadow: hoveredBox === 'analytics-1' ? '0 15px 40px rgba(244, 163, 0, 0.2)' : 'none'
                  }}
                >
                  <h3 className="text-2xl text-[#1C1C1C] mb-10 font-bold" style={{ fontFamily: 'var(--font-head)' }}>Distribution by Type</h3>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Pie data={userTypes} cx="50%" cy="50%" innerRadius={80} outerRadius={110} paddingAngle={5} dataKey="value">
                          {userTypes.map((_, index) => (
                            <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                          ))}
                        </Pie>
                        <Tooltip contentStyle={{ fontSize: '16px', borderRadius: '10px', border: '2px solid #D1D5DB' }} />
                        <Legend wrapperStyle={{ paddingTop: '20px', fontSize: '16px', fontWeight: 'bold' }} />
                      </PieChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                <div 
                  onMouseEnter={() => setHoveredBox('analytics-2')}
                  onMouseLeave={() => setHoveredBox(null)}
                  className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 transition-all duration-300 shadow-xl"
                  style={{
                    borderColor: hoveredBox === 'analytics-2' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'analytics-2' ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                >
                  <h3 className="text-2xl text-[#1C1C1C] mb-10 font-bold" style={{ fontFamily: 'var(--font-head)' }}>Usage Trend</h3>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={usageTrends}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} />
                        <XAxis dataKey="month" tick={{ fontSize: 14, fontWeight: 'bold' }} />
                        <YAxis tick={{ fontSize: 14, fontWeight: 'bold' }} />
                        <Tooltip contentStyle={{ fontSize: '16px', borderRadius: '10px', border: '2px solid #D1D5DB' }} />
                        <Bar dataKey="queries" fill="#2E5BBA" radius={[6, 6, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}