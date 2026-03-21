import { useState } from "react";
import { useNavigate } from "react-router";
import { User, Mail, Building2, Phone, FileText, LogOut, ArrowLeft } from "lucide-react";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function UserProfile() {
  const navigate = useNavigate();
  const [isMainHovered, setIsMainHovered] = useState(false);
  const [hoveredField, setHoveredField] = useState<string | null>(null);
  
  const userData = JSON.parse(
    sessionStorage.getItem("userData") || '{"fullName":"User","email":"user@example.com","orgType":"Student"}'
  );

  const handleLogout = () => {
    sessionStorage.clear();
    navigate("/module-selection");
  };

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        {/* Header */}
        <div className="bg-[#1F3A5F] text-white px-10 py-6 shadow-xl relative z-50 border-b-2 border-[#2E5BBA]">
          <div className="max-w-[1800px] mx-auto flex justify-between items-center">
            <h1 
              className="text-4xl m-0 font-black tracking-tighter cursor-pointer hover:opacity-80 transition-opacity" 
              style={{ fontFamily: 'var(--font-head)', color: 'white' }}
              onClick={() => navigate("/user/dashboard")}
            >
              STATXTRACT
            </h1>
            <div className="flex items-center gap-4 bg-white/10 px-6 py-3 rounded-xl border-2 border-white/20">
              <User className="w-6 h-6 text-white" />
              <span className="font-bold text-xl">{userData.fullName}</span>
            </div>
          </div>
        </div>

        <div className="max-w-[1600px] mx-auto px-10 py-12">
          <div className="mb-10 flex justify-between items-center">
            <button
              onClick={() => navigate("/user/dashboard")}
              className="lift-on-hover flex items-center gap-3 bg-white border-2 border-[#2E5BBA] text-[#1F3A5F] px-8 py-3 rounded-xl transition-all duration-300 hover:bg-[#2E5BBA] hover:text-white cursor-pointer shadow-lg text-xl font-bold"
            >
              <ArrowLeft className="w-6 h-6" />
              <span>Back to Dashboard</span>
            </button>
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>My Profile</h1>
            <div className="w-[180px]"></div>
          </div>

          <div className="max-w-[1000px] mx-auto">
            {/* Profile Card */}
            <div 
              onMouseEnter={() => setIsMainHovered(true)}
              onMouseLeave={() => setIsMainHovered(false)}
              className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl shadow-xl p-12 mb-10 transition-all duration-300"
              style={{
                borderColor: isMainHovered ? '#2E7D32' : '#D1D5DB',
                boxShadow: isMainHovered ? '0 20px 50px rgba(46, 125, 50, 0.2)' : 'none'
              }}
            >
              <div className="flex items-center gap-8 mb-10 pb-10 border-b-4 border-[#2E7D32]/30">
                <div className="w-32 h-32 bg-[#2E7D32] rounded-3xl flex items-center justify-center shadow-lg">
                  <User className="w-16 h-16 text-white" />
                </div>
                <div>
                  <h3 className="text-4xl text-[#1C1C1C] m-0 mb-3 font-black" style={{ fontFamily: 'var(--font-head)' }}>{userData.fullName}</h3>
                  <p className="text-2xl text-[#6B7280] m-0 flex items-center gap-3 font-bold">
                    <Building2 className="w-6 h-6" />
                    {userData.orgType}
                  </p>
                </div>
              </div>

              {/* Profile Details */}
              <div className="space-y-10">
                <div>
                  <label className="block text-[#6B7280] text-sm mb-3 font-black uppercase tracking-widest">
                    Full Name
                  </label>
                  <div 
                    onMouseEnter={() => setHoveredField('name')}
                    onMouseLeave={() => setHoveredField(null)}
                    className="flex items-center gap-5 bg-[#F5F7FA] border-2 rounded-xl px-6 py-5 transition-all duration-300"
                    style={{
                      borderColor: hoveredField === 'name' ? '#2E7D32' : '#E5E7EB',
                      boxShadow: hoveredField === 'name' ? '0 10px 20px rgba(46, 125, 50, 0.1)' : 'none'
                    }}
                  >
                    <User className="w-8 h-8 text-[#2E7D32]" />
                    <p className="text-2xl text-[#1C1C1C] m-0 font-bold">{userData.fullName}</p>
                  </div>
                </div>

                <div>
                  <label className="block text-[#6B7280] text-sm mb-3 font-black uppercase tracking-widest">
                    Email Address
                  </label>
                  <div 
                    onMouseEnter={() => setHoveredField('email')}
                    onMouseLeave={() => setHoveredField(null)}
                    className="flex items-center gap-5 bg-[#F5F7FA] border-2 rounded-xl px-6 py-5 transition-all duration-300"
                    style={{
                      borderColor: hoveredField === 'email' ? '#2E7D32' : '#E5E7EB',
                      boxShadow: hoveredField === 'email' ? '0 10px 20px rgba(46, 125, 50, 0.1)' : 'none'
                    }}
                  >
                    <Mail className="w-8 h-8 text-[#2E7D32]" />
                    <p className="text-2xl text-[#1C1C1C] m-0 font-bold">{userData.email}</p>
                  </div>
                </div>

                <div>
                  <label className="block text-[#6B7280] text-sm mb-3 font-black uppercase tracking-widest">
                    Organization Type
                  </label>
                  <div 
                    onMouseEnter={() => setHoveredField('org')}
                    onMouseLeave={() => setHoveredField(null)}
                    className="flex items-center gap-5 bg-[#F5F7FA] border-2 rounded-xl px-6 py-5 transition-all duration-300"
                    style={{
                      borderColor: hoveredField === 'org' ? '#2E7D32' : '#E5E7EB',
                      boxShadow: hoveredField === 'org' ? '0 10px 20px rgba(46, 125, 50, 0.1)' : 'none'
                    }}
                  >
                    <Building2 className="w-8 h-8 text-[#2E7D32]" />
                    <p className="text-2xl text-[#1C1C1C] m-0 font-bold">{userData.orgType}</p>
                  </div>
                </div>

                {userData.phone && (
                  <div>
                    <label className="block text-[#6B7280] text-sm mb-3 font-black uppercase tracking-widest">
                      Phone Number
                    </label>
                    <div 
                      onMouseEnter={() => setHoveredField('phone')}
                      onMouseLeave={() => setHoveredField(null)}
                      className="flex items-center gap-5 bg-[#F5F7FA] border-2 rounded-xl px-6 py-5 transition-all duration-300"
                      style={{
                        borderColor: hoveredField === 'phone' ? '#2E7D32' : '#E5E7EB',
                        boxShadow: hoveredField === 'phone' ? '0 10px 20px rgba(46, 125, 50, 0.1)' : 'none'
                      }}
                    >
                      <Phone className="w-8 h-8 text-[#2E7D32]" />
                      <p className="text-2xl text-[#1C1C1C] m-0 font-bold">{userData.phone}</p>
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Logout Button */}
            <button
              onClick={handleLogout}
              className="lift-on-hover w-full flex items-center justify-center gap-4 bg-[#d4183d] text-white py-6 rounded-2xl border-2 border-[#d4183d] hover:border-[#F4A300] cursor-pointer transition-all duration-300 hover:bg-[#b01530] shadow-xl text-2xl font-black"
            >
              <LogOut className="w-8 h-8" />
              <span>Logout</span>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
