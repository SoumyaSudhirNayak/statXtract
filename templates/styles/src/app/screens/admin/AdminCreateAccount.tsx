import { useState, useRef } from "react";
import { useNavigate } from "react-router";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function AdminCreateAccount() {
  const navigate = useNavigate();
  const [formData, setFormData] = useState({
    fullName: "",
    officialEmail: "",
    phoneNumber: "",
    employeeId: "",
    organizationName: "",
    department: "",
    designation: "",
  });
  const [isContainerHovered, setIsContainerHovered] = useState(false);
  const [hoveredInput, setHoveredInput] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    // Store admin data in session storage for demo
    sessionStorage.setItem("adminData", JSON.stringify(formData));
    navigate("/admin/dashboard");
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData({ ...formData, [e.target.name]: e.target.value });
  };

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <div className="bg-[#1F3A5F] text-white px-10 py-6 shadow-xl relative z-50">
          <div className="max-w-[1800px] mx-auto flex justify-between items-center">
            <h1 
              className="text-4xl m-0 font-black tracking-tighter cursor-pointer hover:opacity-80 transition-opacity" 
              style={{ fontFamily: 'var(--font-head)', color: 'white' }}
              onClick={() => navigate("/")}
            >
              STATXTRACT
            </h1>
          </div>
        </div>

        <div className="max-w-[1200px] mx-auto px-10 py-16">
          <div className="mb-10 flex justify-between items-center">
            <BackButton to="/admin/auth" />
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Create Admin Account</h1>
            <div className="w-[80px]"></div>
          </div>

          <form 
            ref={containerRef}
            onSubmit={handleSubmit} 
            onMouseEnter={() => setIsContainerHovered(true)}
            onMouseLeave={() => setIsContainerHovered(false)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 relative overflow-hidden transition-all duration-300 shadow-xl"
            style={{
              borderColor: isContainerHovered ? '#2E5BBA' : '#D1D5DB',
              boxShadow: isContainerHovered 
                ? '0 20px 50px rgba(46, 91, 186, 0.2)'
                : 'none',
            }}
          >
            <div className="space-y-10 relative z-10">
              <div>
                <label className="block text-[#1C1C1C] mb-3 font-bold text-xl">Full Name</label>
                <input
                  type="text"
                  name="fullName"
                  value={formData.fullName}
                  onChange={handleChange}
                  onMouseEnter={() => setHoveredInput('fullName')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredInput === 'fullName' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredInput === 'fullName' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 font-bold text-xl">Official Email</label>
                <input
                  type="email"
                  name="officialEmail"
                  value={formData.officialEmail}
                  onChange={handleChange}
                  onMouseEnter={() => setHoveredInput('officialEmail')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredInput === 'officialEmail' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredInput === 'officialEmail' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                <div>
                  <label className="block text-[#1C1C1C] mb-3 font-bold text-xl">Phone Number</label>
                  <input
                    type="tel"
                    name="phoneNumber"
                    value={formData.phoneNumber}
                    onChange={handleChange}
                    onMouseEnter={() => setHoveredInput('phoneNumber')}
                    onMouseLeave={() => setHoveredInput(null)}
                    required
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                    style={{
                      borderColor: hoveredInput === 'phoneNumber' ? '#2E5BBA' : '#D1D5DB',
                      boxShadow: hoveredInput === 'phoneNumber' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                    }}
                  />
                </div>
                <div>
                  <label className="block text-[#1C1C1C] mb-3 font-bold text-xl">Employee ID</label>
                  <input
                    type="text"
                    name="employeeId"
                    value={formData.employeeId}
                    onChange={handleChange}
                    onMouseEnter={() => setHoveredInput('employeeId')}
                    onMouseLeave={() => setHoveredInput(null)}
                    required
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                    style={{
                      borderColor: hoveredInput === 'employeeId' ? '#2E5BBA' : '#D1D5DB',
                      boxShadow: hoveredInput === 'employeeId' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                    }}
                  />
                </div>
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 font-bold text-xl">Organization Name</label>
                <input
                  type="text"
                  name="organizationName"
                  value={formData.organizationName}
                  onChange={handleChange}
                  onMouseEnter={() => setHoveredInput('organizationName')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredInput === 'organizationName' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredInput === 'organizationName' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                <div>
                  <label className="block text-[#1C1C1C] mb-3 font-bold text-xl">Department</label>
                  <input
                    type="text"
                    name="department"
                    value={formData.department}
                    onChange={handleChange}
                    onMouseEnter={() => setHoveredInput('department')}
                    onMouseLeave={() => setHoveredInput(null)}
                    required
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                    style={{
                      borderColor: hoveredInput === 'department' ? '#2E5BBA' : '#D1D5DB',
                      boxShadow: hoveredInput === 'department' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                    }}
                  />
                </div>
                <div>
                  <label className="block text-[#1C1C1C] mb-3 font-bold text-xl">Designation</label>
                  <input
                    type="text"
                    name="designation"
                    value={formData.designation}
                    onChange={handleChange}
                    onMouseEnter={() => setHoveredInput('designation')}
                    onMouseLeave={() => setHoveredInput(null)}
                    required
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                    style={{
                      borderColor: hoveredInput === 'designation' ? '#2E5BBA' : '#D1D5DB',
                      boxShadow: hoveredInput === 'designation' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                    }}
                  />
                </div>
              </div>

              <button
                type="submit"
                onMouseEnter={() => setHoveredInput('submit')}
                onMouseLeave={() => setHoveredInput(null)}
                className="lift-on-hover w-full bg-[#2E5BBA] text-white py-5 rounded-xl border-2 cursor-pointer transition-all duration-300 text-2xl font-black mt-6 shadow-lg"
                style={{
                  borderColor: hoveredInput === 'submit' ? '#F4A300' : '#2E5BBA',
                  boxShadow: hoveredInput === 'submit' ? '0 10px 25px rgba(244, 163, 0, 0.4)' : 'none'
                }}
              >
                Create Admin Account
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}